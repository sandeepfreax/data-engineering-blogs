"""
Medallion Architecture — Layer 2 of 3

PHILOSOPHY:
    Silver is where raw data becomes trustworthy data.
    Bronze was about preservation — Silver is about CONTRACT.

    A Silver table makes these guarantees to downstream consumers:
      - Column names are clean snake_case — no surprises
      - Data types are correct — dates are dates, numbers are numbers
      - Suppressed values are explicit — is_suppressed flag, not silent nulls
      - Duplicates are eliminated — idempotent re-runs produce same result
      - Partitioned by business time — filter by ref_year/ref_month, not ingest time
      - Schema is enforced — Delta rejects writes that break the contract

WHAT SILVER DOES NOT DO:
      x No business aggregations (that is Gold's job)
      ✗ No joining across tables (that is Gold's job)
      ✗ No KPIs, no rates calculated (Gold + dbt)

DEDUPLICATION STRATEGY:
    We use Window-based row_number() rather than dropDuplicates().
    Why?
      - dropDuplicates() compares ALL columns — fragile if StatCan adds a col
      - Window approach deduplicates on NATURAL KEY columns explicitly
      - We can control which row "wins" (latest _ingested_at wins)
      - Makes deduplication logic visible, testable, and documentable
    Natural key for lfs_province:
      (ref_date, geo, labour_force_characteristic, gender, age_group, statistic_type, data_type)
    Natural key for lfs_industry:
      (ref_date, geo, naics_industry, statistic_type, data_type)

STATUS / NULL HANDLING:
    StatCan uses STATUS column to explain why VALUE is null:
      ".."  = data suppressed for confidentiality (sample size too small)
      "x"   = data too unreliable to publish
      null  = VALUE is present and valid (most rows)
    We NEVER drop these rows. We add an explicit is_suppressed boolean flag.
    Downstream consumers (dbt, analysts) can then filter appropriately.
    Blindly dropping nulls here would silently corrupt regional analysis (small provinces like PEI have high suppression rates).

REF_DATE CASTING:
    Source format: "1976-01" (year-month string)
    Target format: DATE "1976-01-01" (first day of reference month)
    Why first-of-month? Consistent, sortable, compatible with date_trunc().
    We also extract ref_year and ref_month as partition columns.

"""

import os
from datetime import datetime
from pathlib import Path

from loguru import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# ── Path config ──────────────────────────────────────────────────────────────
DELTA_BASE_PATH      = Path(os.getenv("DELTA_BASE_PATH", str(PROJECT_ROOT / "delta_tables")))
BRONZE_PROVINCE_PATH = str(DELTA_BASE_PATH / "bronze" / "lfs_province")
BRONZE_INDUSTRY_PATH = str(DELTA_BASE_PATH / "bronze" / "lfs_industry")
SILVER_PROVINCE_PATH = str(DELTA_BASE_PATH / "silver" / "lfs_province")
SILVER_INDUSTRY_PATH = str(DELTA_BASE_PATH / "silver" / "lfs_industry")

# ── Natural dedup keys ───────────────────────────────────────────────────────
# These columns together uniquely identify one observation in each table.
# If two rows share ALL these values, one is a duplicate.
PROVINCE_NATURAL_KEY = [
    "ref_date",
    "geo",
    "labour_force_characteristic",
    "gender",
    "age_group",
    "statistic_type",
    "data_type",
]

INDUSTRY_NATURAL_KEY = [
    "ref_date",
    "geo",
    "naics_industry",
    "statistic_type",
    "data_type",
]

# ── Column rename maps (Bronze sanitized name → Silver snake_case name) ──────
# Bronze preserves source names (with _ instead of spaces).
# Silver renames to clean, consistent, lowercase snake_case.
PROVINCE_RENAME_MAP = {
    "REF_DATE":                      "ref_date_raw",
    "GEO":                           "geo",
    "DGUID":                         "dguid",
    "Labour_force_characteristics":  "labour_force_characteristic",
    "Gender":                        "gender",
    "Age_group":                     "age_group",
    "Statistics":                    "statistic_type",
    "Data_type":                     "data_type",
    "UOM":                           "unit_of_measure",
    "SCALAR_FACTOR":                 "scalar_factor",
    "VECTOR":                        "vector_id",
    "COORDINATE":                    "coordinate",
    "VALUE":                         "value_raw",
    "STATUS":                        "status",
    "DECIMALS":                      "decimals",
    "_ingested_at":                  "_ingested_at",
    "_source_file":                  "_source_file",
}

INDUSTRY_RENAME_MAP = {
    "REF_DATE":                                               "ref_date_raw",
    "GEO":                                                    "geo",
    "DGUID":                                                  "dguid",
    "North_American_Industry_Classification_System_NAICS":    "naics_industry",
    "Statistics":                                             "statistic_type",
    "Data_type":                                              "data_type",
    "SCALAR_FACTOR":                                          "scalar_factor",
    "VECTOR":                                                 "vector_id",
    "COORDINATE":                                             "coordinate",
    "VALUE":                                                  "value_raw",
    "STATUS":                                                 "status",
    "DECIMALS":                                               "decimals",
    "_ingested_at":                                           "_ingested_at",
    "_source_file":                                           "_source_file",
}


# ── SparkSession ─────────────────────────────────────────────────────────────

def get_spark(app_name: str = "silver_transform") -> SparkSession:
    # Enable Hive support warehouse path — must match dbt profiles.yml
    warehouse_dir = str(PROJECT_ROOT / "spark-warehouse")
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        # ── Fix: timestamp timezone parsing ─────────────────────
        # Enables Java 8 time API which correctly handles timezone offsets like "-01" and "+05:30" in stored timestamps.
        # Without this, Spark's legacy Proleptic Gregorian calendar parser fails on timezone-offset timestamp strings
        # produced by current_timestamp() on non-UTC machines.
        .config("spark.sql.datetime.java8API.enabled", "true")

        # Treat timestamps as UTC internally — avoids local Machine timezone ambiguity when reading back from Parquet/Delta
        .config("spark.sql.session.timeZone", "UTC")
        # ─────────────────────────────────────────────────────────
        # CORRECTED = strict ISO-8601; bad timestamps → null (safe)
        # LEGACY    = Spark 2.x lenient parsing; can silently produce wrong dates
        # EXCEPTION = crash on bad timestamps
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── Step 1: Read Bronze ──────────────────────────────────────────────────────

def read_bronze(spark: SparkSession, bronze_path: str, table_name: str) -> DataFrame:
    """
    Read from Bronze Delta table.

    IMPORTANT: inferSchema in Bronze coerced REF_DATE ("1976-01") to TimestampType as "1976-01-01 00:00:00".
    We cast it back to string here so cast_ref_date() receives a clean "yyyy-MM-dd" string it can reliably parse. We
    only need the yyyy-MM portion, so we extract that.
    """
    logger.info(f"  Reading Bronze: {bronze_path}")
    df = spark.read.format("delta").load(bronze_path)

    # Cast REF_DATE back to the yyyy-MM string it actually represents.
    # Bronze inferSchema promoted "1976-01" → TimestampType "1976-01-01 00:00:00".
    # We reverse this before any Silver transformations run.
    df = df.withColumn("REF_DATE", F.date_format(F.col("REF_DATE").cast("timestamp"), "yyyy-MM"))

    logger.info(f"  Bronze rows  : {df.count():,}")
    logger.info(f"  Bronze cols  : {len(df.columns)}")
    return df


# ── Step 2: Rename columns ───────────────────────────────────────────────────

def rename_columns(df: DataFrame, rename_map: dict, table_name: str) -> DataFrame:
    """
    Rename Bronze sanitized names to Silver snake_case business names.

    This is intentionally separate from sanitization (which happened at Bronze).
    Bronze sanitization was a technical necessity (Delta constraints).
    Silver renaming is a business decision (naming convention contract).

    We use rename_map for explicit, documented mapping — not generic regex.
    """
    logger.info(f"  Renaming columns for {table_name}")

    # Only rename columns that exist in the DataFrame (guards against schema drift between Bronze runs)
    existing_cols = set(df.columns)
    select_exprs = []
    missing_cols = []

    for bronze_name, silver_name in rename_map.items():
        if bronze_name in existing_cols:
            select_exprs.append(F.col(f"`{bronze_name}`").alias(silver_name))
        else:
            missing_cols.append(bronze_name)

    # Fail fast — do NOT silently skip. A missing rename = downstream column resolution failure that surfaces much
    # later and is harder to debug.
    if missing_cols:
        available = "\n    ".join(sorted(existing_cols))
        raise ValueError(
            f"\n\n[rename_columns] {table_name}: {len(missing_cols)} column(s) "
            f"in rename_map not found in Bronze DataFrame.\n"
            f"  Missing keys : {missing_cols}\n"
            f"  Available    :\n    {available}\n"
            f"  → Update INDUSTRY_RENAME_MAP keys to match Bronze column names exactly."
        )

    return df.select(select_exprs)


# ── Step 3: Cast REF_DATE ────────────────────────────────────────────────────

def cast_ref_date(df: DataFrame) -> DataFrame:
    """
    Cast ref_date_raw from "1976-01" string to DATE "1976-01-01".

    Format "yyyy-MM" parsed to first day of month by appending "-01".
    concat_ws("-", col, lit("01")) → "1976-01-01" → to_date("yyyy-MM-dd")

    Why not to_date(ref_date_raw, "yyyy-MM") directly?
    Spark's to_date with "yyyy-MM" format assumes the 1st of the month internally but is unreliable across Spark
    versions with CORRECTED policy. Explicit "-01" append + "yyyy-MM-dd" format is version-deterministic.
    """
    return df.withColumn("ref_date",
        F.to_date(F.concat_ws("-", F.col("ref_date_raw"), F.lit("01")),"yyyy-MM-dd"))


# ── Step 4: Handle STATUS / suppression ─────────────────────────────────────

def handle_suppression(df: DataFrame) -> DataFrame:
    """
    Make VALUE suppression explicit with a boolean flag.

    StatCan STATUS codes:
      ".."   = data suppressed (sample too small for reliable estimate)
      "x"    = data too unreliable to publish
      null   = VALUE is present and valid

    Design decision:
      We do NOT fill suppressed values with 0 or mean — that would fabricate data.
      We do NOT drop suppressed rows — that would make small provinces disappear.
      We ADD is_suppressed flag so consumers can explicitly filter or handle.

    CRITICAL observations:
      PEI, NWT, Yukon have high suppression rates due to small populations.
      Silently dropping null VALUE rows = silently removing these regions
      from any regional analysis. This is a real data quality trap.

    VALUE column:
      - Where is_suppressed = True  → value_cleaned = null (preserve null)
      - Where is_suppressed = False → value_cleaned = value_raw (the actual number)
      We cast to DoubleType explicitly — inferSchema may have read as string if suppression codes appeared in VALUE
        (they shouldn\'t, but guard anyway).
    """
    df = df.withColumn("is_suppressed", F.col("status").isin("..", "x"))

    df = df.withColumn("is_suppressed", F.coalesce(F.col("is_suppressed").cast("boolean"), F.lit(False)))

    df = df.withColumn("value",
        F.when(F.col("is_suppressed"), F.lit(None).cast(DoubleType()))
         .otherwise(F.col("value_raw").cast(DoubleType())))

    return df


# ── Step 5: Add partition columns ────────────────────────────────────────────

def add_partition_columns(df: DataFrame) -> DataFrame:
    """
    Add ref_year and ref_month as partition columns for Silver.

    Bronze was partitioned by ingestion_year/ingestion_month.
    Silver switches to DATA time — when the observation was recorded.
    This is expected partition strategy for analytical workloads.

    ref_month: zero-padded 2-digit string "01"-"12"
    ref_year : 4-digit string "1976"-"2026"
    """
    return df.withColumns({
        "ref_year":  F.date_format(F.col("ref_date"), "yyyy"),
        "ref_month": F.date_format(F.col("ref_date"), "MM"),
    })


# ── Step 6: Deduplicate ──────────────────────────────────────────────────────

def deduplicate(df: DataFrame, natural_key: list, table_name: str) -> DataFrame:
    """
    Deduplicate using Window row_number() on natural key columns.

    Why Window-based dedup over dropDuplicates()?
    dropDuplicates() compares ALL columns. If StatCan adds a new column in a future release, two rows that were
    duplicates before (same observation, same value) become non-duplicates because the new column differs. This is
    schema-drift-fragile deduplication.

    Window-based approach deduplicates on EXPLICIT natural key columns only.
    We control which row wins: latest _ingested_at (most recent Bronze ingest).
    This is idempotent — running Silver twice produces the same output.

    For lfs_province, natural key:
      (ref_date, geo, labour_force_characteristic, gender, age_group, statistic_type, data_type)

    For lfs_industry, natural key:
      (ref_date, geo, naics_industry, statistic_type, data_type)
    """
    logger.info(f"  Deduplicating {table_name} on {len(natural_key)} key columns")
    logger.info(f"  Key: {natural_key}")

    before_count = df.count()

    window = Window.partitionBy(natural_key).orderBy(F.col("_ingested_at").desc())

    df_deduped = (
        df.withColumn("_row_num", F.row_number().over(window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )

    after_count = df_deduped.count()
    duplicates  = before_count - after_count

    logger.info(f"  Before dedup : {before_count:,}")
    logger.info(f"  After dedup  : {after_count:,}")
    if duplicates > 0:
        logger.warning(f"  Removed dupes: {duplicates:,}")
    else:
        logger.success(f"  No duplicates found")

    return df_deduped


# ── Step 7: Drop staging columns ─────────────────────────────────────────────

def drop_staging_columns(df: DataFrame) -> DataFrame:
    """
    Drop columns used for transformation logic but not needed in Silver.

    ref_date_raw : The original "1976-01" string. Now redundant since ref_date (DATE type) is the authoritative column.
    value_raw    : The raw float before suppression handling. Redundant since value (DoubleType with suppression applied) is clean.

    We keep these through all transformation steps so we can verify our casts are correct. Only drop at the very end of the pipeline.
    """
    return df.drop("ref_date_raw", "value_raw")


# ── Step 8: Write Silver Delta ───────────────────────────────────────────────

def write_silver_delta(df: DataFrame, silver_path: str, table_name: str) -> None:
    """
    Write to Silver Delta Lake.

    Mode : overwrite (full refresh from Bronze on each run)
    Partition : ref_year / ref_month (business time, not ingestion time)

    Schema enforcement:
      Unlike Bronze which used overwriteSchema=true,
      Silver uses mergeSchema=false (the default, strict enforcement).
      Once the Silver schema is established, we REJECT any write that tries to change it without an intentional schema
      evolution decision. This protects downstream dbt models from silent schema drift.

    Delta constraint:
      Add a CHECK constraint ensuring value is always non-negative.
      Employment counts cannot be negative — this catches upstream data errors.
    """
    logger.info(f"  Writing Silver Delta: {silver_path}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")    # first run needs this; remove after v1
        .partitionBy("ref_year", "ref_month")
        .save(silver_path)
    )

    # Add Delta CHECK constraint — value must be non-negative or null (null is valid — it means suppressed)
    spark = SparkSession.getActiveSession()
    try:
        spark.sql(f"""
            ALTER TABLE delta.`{silver_path}`
            ADD CONSTRAINT value_non_negative
            CHECK (value IS NULL OR value >= 0)
        """)
        logger.success(f"  Constraint added: value IS NULL OR value >= 0")
    except Exception as e:
        # Constraint may already exist on re-runs
        if "already exists" in str(e).lower():
            logger.info(f"  Constraint already exists — skipping")
        else:
            logger.warning(f"  Could not add constraint: {e}")

    # Verify write
    df_verify = spark.read.format("delta").load(silver_path)
    written   = df_verify.count()
    logger.success(f"  Rows written : {written:,}")

    from delta.tables import DeltaTable
    version = DeltaTable.forPath(spark, silver_path).history(1).select("version").collect()[0][0]
    logger.success(f"  Delta version: {version}")


def log_silver_schema(df: DataFrame, table_name: str) -> None:
    """Print final Silver schema."""
    print(f"\\n  Final Silver Schema — {table_name}")
    print("  " + "─" * 60)
    for field in df.schema.fields:
        nullable = "nullable" if field.nullable else "NOT NULL"
        print(f"  {field.name:<45} {str(field.dataType):<15} {nullable}")
    print()


def show_sample(df: DataFrame, table_name: str, n: int = 5) -> None:
    """Show sample rows — verify transforms look correct."""
    print(f"\\n  Sample rows — {table_name}")
    print("  " + "─" * 60)
    sample_cols = [
        c for c in [
            "ref_date", "geo", "labour_force_characteristic", "naics_industry", "gender", "age_group", "data_type",
            "value", "status", "is_suppressed", "ref_year", "ref_month"
        ] if c in df.columns
    ]
    df.select(sample_cols).show(n, truncate=35)


def register_silver_tables(spark: SparkSession) -> None:
    """
    Register Silver Delta tables as permanent Spark SQL catalog entries.

    WHY here and not in a separate script:
      - SparkSession is already live with Delta extensions configured
      - SILVER_PROVINCE_PATH and SILVER_INDUSTRY_PATH are already resolved
      - Guarantees registration always happens after a successful Silver write
      - dbt-spark (method: session) starts its own SparkSession but reads from the same local Hive metastore
        (spark-warehouse/) — so tables registered here persist and are visible to dbt build

    CREATE TABLE IF NOT EXISTS ... USING delta LOCATION:
      Registers the Delta path in the Spark catalog without copying data.
      The table entry is a pointer — all reads go directly to Delta files.
      Safe to re-run — IF NOT EXISTS prevents duplicate registration errors.
    """
    logger.info("\n" + "═" * 60)
    logger.info("  CATALOG — Registering Silver tables for dbt")
    logger.info("═" * 60)

    tables = {
        "silver_lfs_province": SILVER_PROVINCE_PATH,
        "silver_lfs_industry": SILVER_INDUSTRY_PATH,
    }

    for table_name, delta_path in tables.items():
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING delta
            LOCATION '{delta_path}'
        """)
        count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
        logger.success(f"{table_name}: {count:,} rows registered")

    logger.success("  Tables ready — now you can run `cd dbt && dbt build`")


# ── Table-level orchestrators ─────────────────────────────────────────────────

def transform_lfs_province(spark: SparkSession) -> None:
    logger.info("\\n" + "═" * 60)
    logger.info("  SILVER — lfs_province")
    logger.info("═" * 60)

    df = read_bronze(spark, BRONZE_PROVINCE_PATH, "lfs_province")
    df = rename_columns(df, PROVINCE_RENAME_MAP, "lfs_province")
    df = cast_ref_date(df)
    df = handle_suppression(df)
    df = add_partition_columns(df)
    df = deduplicate(df, PROVINCE_NATURAL_KEY, "lfs_province")
    df = drop_staging_columns(df)
    log_silver_schema(df, "lfs_province")
    show_sample(df, "lfs_province")
    write_silver_delta(df, SILVER_PROVINCE_PATH, "lfs_province")
    df.unpersist()


def transform_lfs_industry(spark: SparkSession) -> None:
    logger.info("\\n" + "═" * 60)
    logger.info("  SILVER — lfs_industry")
    logger.info("═" * 60)

    df = read_bronze(spark, BRONZE_INDUSTRY_PATH, "lfs_industry")
    df = rename_columns(df, INDUSTRY_RENAME_MAP, "lfs_industry")
    df = cast_ref_date(df)
    df = handle_suppression(df)
    df = add_partition_columns(df)
    df = deduplicate(df, INDUSTRY_NATURAL_KEY, "lfs_industry")
    df = drop_staging_columns(df)
    log_silver_schema(df, "lfs_industry")
    show_sample(df, "lfs_industry")
    write_silver_delta(df, SILVER_INDUSTRY_PATH, "lfs_industry")
    df.unpersist()


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    start_time = datetime.now()

    logger.info("╔" + "═" * 58 + "╗")
    logger.info("║  spark-lakehouse-statcan | Silver Layer Transform       ║")
    logger.info("╚" + "═" * 58 + "╝")
    logger.info(f"  Started    : {start_time}")
    logger.info(f"  Delta base : {DELTA_BASE_PATH}")

    spark = get_spark("silver_transform")
    logger.info(f"  Spark ver  : {spark.version}")

    try:
        transform_lfs_province(spark)
        transform_lfs_industry(spark)
        register_silver_tables(spark)

        elapsed = (datetime.now() - start_time).seconds
        logger.success(f"\\nSilver transform complete in {elapsed}s")

    except Exception as e:
        logger.error(f"\\nSilver transform failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()