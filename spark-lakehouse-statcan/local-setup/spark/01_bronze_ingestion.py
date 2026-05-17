"""
Medallion Architecture — Layer 1 of 3

PHILOSOPHY:
    Bronze is sacred. We write raw data exactly as received from the source.
    No transformations. No type casting. No business logic.
    The only additions are audit columns (_ingested_at, _source_file).

    Why? Because if Silver/Gold logic has a bug, we can always reprocess
    from Bronze without re-downloading from StatCan.

TABLES INGESTED:
    1. lfs_province  → delta_tables/bronze/lfs_province/
       Source: data/raw/lfs_province/14100287.csv
       Rows  : ~5.4M | Columns: 15 (after dropping 4 internal/null cols)

    2. lfs_industry  → delta_tables/bronze/lfs_industry/
       Source: data/raw/lfs_industry/14100355.csv
       Rows  : ~683K  | Columns: 12 (after dropping 5 internal/null cols)

PARTITION STRATEGY:
    Partitioned by: ingestion_year, ingestion_month
    Why NOT by REF_DATE at Bronze?
    - REF_DATE is a string "1976-01" at this stage — not yet cast to a date
    - We partition by ingestion time to track WHEN data entered our system
    - REF_DATE partitioning happens in Silver, where we control the cast

COLUMNS DROPPED AT BRONZE (documented intentionally):
    - SYMBOL     : 100% null in both tables — no information value
    - TERMINATED : 100% null in both tables — no information value
    - UOM_ID     : Internal StatCan numeric code, redundant with UOM text column
    - SCALAR_ID  : Internal StatCan numeric code, redundant with SCALAR_FACTOR text column (lfs_industry only)
    - UOM        : Single value \'Persons in thousands\' across all rows — constant

NOTE ON VALUE NULLS (26-27%):
    VALUE is null where STATUS is \'..\' (suppressed) or \'x\' (confidential).
    These are NOT data quality issues — they are StatCan privacy protections.
    We preserve STATUS as-is in Bronze. Silver will handle these appropriately.
"""

import os
from datetime import datetime
from pathlib import Path

from loguru import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from dotenv import load_dotenv

# Project root resolution — works from any working directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# ── Path config ─────────────────────────────────────────────────────────────
RAW_DATA_PATH   = PROJECT_ROOT / "data" / "raw"
DELTA_BASE_PATH = Path(os.getenv("DELTA_BASE_PATH",
                                  str(PROJECT_ROOT / "delta_tables")))

BRONZE_PROVINCE_PATH = str(DELTA_BASE_PATH / "bronze" / "lfs_province")
BRONZE_INDUSTRY_PATH = str(DELTA_BASE_PATH / "bronze" / "lfs_industry")


# ── Column definitions ───────────────────────────────────────────────────────

# Columns to DROP from lfs_province
PROVINCE_DROP_COLS = [
    "UOM_ID",       # internal StatCan numeric code — redundant with UOM
    "SCALAR_ID",    # internal StatCan numeric code — redundant with SCALAR_FACTOR
    "SYMBOL",       # 100% null across all 5.4M rows
    "TERMINATED",   # 100% null across all 5.4M rows
]

# Columns to DROP from lfs_industry
INDUSTRY_DROP_COLS = [
    "UOM",          # single constant value \'Persons in thousands\' — no variance
    "UOM_ID",       # internal StatCan numeric code
    "SCALAR_ID",    # internal StatCan numeric code
    "SYMBOL",       # 100% null across all 683K rows
    "TERMINATED",   # 100% null across all 683K rows
]


# ── SparkSession ─────────────────────────────────────────────────────────────

def get_spark(app_name: str = "bronze_ingestion") -> SparkSession:
    """
    Local SparkSession with Delta Lake support.
    First run downloads Delta JAR (~15 MB) — cached at ~/.ivy2 for all future runs.
    """

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        # ── Fix: timestamp timezone parsing ─────────────────────
        # Enables Java 8 time API which correctly handles timezone
        # offsets like "-01" and "+05:30" in stored timestamps.
        # Without this, Spark's legacy Proleptic Gregorian calendar
        # parser fails on timezone-offset timestamp strings produced
        # by current_timestamp() on non-UTC machines.
        .config("spark.sql.datetime.java8API.enabled", "true")

        # Treat timestamps as UTC internally — avoids local machine timezone ambiguity when reading back from Parquet/Delta
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── Core ingestion functions ─────────────────────────────────────────────────

def read_statcan_csv(spark: SparkSession, csv_path: Path, table_name: str) -> DataFrame:
    """
    Read a StatCan CSV into a Spark DataFrame.

    BOM Handling:
        StatCan CSVs are UTF-8 with BOM. PySpark does not support 'UTF-8-BOM' as an encoding string — it only accepts
        plain 'UTF-8'. We strip the BOM using Python's built-in utf-8-sig codec into a temp file, then let Spark read
        the clean UTF-8 file.

    Lazy Evaluation Trap:
        Spark is LAZY. spark.read.csv() builds a plan but reads NO data.
        Every subsequent ACTION (count, write, show) re-reads from the source path in the lineage DAG.

        If we delete the temp file after count() but before write(), Spark raises SparkFileNotFoundException on write —
        even though count() already succeeded.

        Fix: call df.persist() BEFORE deleting the temp file.
        persist() forces Spark to materialise the DataFrame into memory,  breaking the lineage dependency on the temp
        file path. After persist(), deleting the temp file is safe.
    """
    import tempfile, shutil

    logger.info(f"  Reading CSV  : {csv_path.name}")

    # ── Step 1: Strip BOM using Python (utf-8-sig handles this natively) ──
    tmp_dir = Path(tempfile.mkdtemp())
    tmp_path = tmp_dir / csv_path.name

    logger.info(f"  Stripping BOM → temp file: {tmp_path}")
    with open(csv_path, encoding="utf-8-sig") as src, \
            open(tmp_path, "w", encoding="utf-8") as dst:
        shutil.copyfileobj(src, dst)

    # ── Step 2: Spark reads clean UTF-8 file ──────────────────────────────
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .option("nullValue", "")
        .option("mode", "PERMISSIVE")
        .csv(str(tmp_path))
    )

    # Step 3 — PERSIST before deleting temp file
    # This materialises the DataFrame into Spark memory, breaking the lineage dependency on tmp_path. Safe to delete after this.
    from pyspark import StorageLevel
    df = df.persist(StorageLevel.MEMORY_AND_DISK)

    # Step 4 — Trigger materialisation with count (forces persist to execute)
    row_count = df.count()
    logger.info(f"  Rows read    : {row_count:,}")
    logger.info(f"  Columns      : {len(df.columns)}")

    # Step 5 — NOW safe to delete temp file (lineage is broken by persist)
    shutil.rmtree(tmp_dir)
    logger.info(f"  Temp file removed")

    return df


def add_audit_columns(df: DataFrame, source_file: str) -> DataFrame:
    """
    Add Bronze audit columns — the ONLY additions Bronze makes to raw data.

    _ingested_at : Timestamp of when this row entered the Bronze layer.
                   Be advised that we have already set UTC as timezone while initializing Spark Session

    _source_file : Which CSV produced this row.
                   Critical for debugging when StatCan releases corrections.
    """
    return df.withColumns({
        "_ingested_at": F.to_timestamp(
                    F.date_format(F.current_timestamp(),"yyyy-MM-dd HH:mm:ss"),
                    "yyyy-MM-dd HH:mm:ss"),
        "_source_file": F.lit(source_file),
    })


def drop_columns(df: DataFrame, cols_to_drop: list, table_name: str) -> DataFrame:
    """Log each for transparency."""
    logger.info(f"  Dropping {len(cols_to_drop)} columns from {table_name}:")
    for col in cols_to_drop:
        logger.info(f"    ✗ {col}")
    existing = [c for c in cols_to_drop if c in df.columns]
    return df.drop(*existing)


def sanitize_column_names(df: DataFrame) -> DataFrame:
    """
    Sanitize column names for Delta Lake compatibility.

    Delta Lake rejects column names containing: ' ,;{}()\\n\\t='
    Rule applied: replace any non-alphanumeric character (except _) with _. then strip leading/trailing underscores,
                  then collapse multiple consecutive underscores to one.

    Examples from StatCan LFS schema:
      'Labour force characteristics' → 'Labour_force_characteristics'
      'Age group' → 'Age_group'
      'North American Industry Classification System (NAICS)'  → 'North_American_Industry_Classification_System_NAICS'
      'Data type'  → 'Data_type'
      'REF_DATE' → 'REF_DATE' (unchanged)
      'VALUE'  → 'VALUE' (unchanged)

    NOTE: We sanitize at Bronze — NOT rename to business names.
    Renaming to snake_case business names (e.g. labour_force_characteristic) happens in Silver, where we intentionally
    apply schema conventions.
    This keeps Bronze as close to source as possible while satisfying Delta's technical constraints.
    """
    import re

    def clean(col_name: str) -> str:
        cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", col_name)  # replace unwanted chars with _
        cleaned = re.sub(r"_+", "_", cleaned)  # collapse multiple underscores
        cleaned = cleaned.strip("_") # strip leading/trailing _
        return cleaned

    renamed = [F.col(f"`{c}`").alias(clean(c)) for c in df.columns]
    return df.select(renamed)

def write_bronze_delta(df: DataFrame, delta_path: str, table_name: str) -> None:
    """
    Write to Bronze Delta Lake.

    Mode: overwrite (full reload on each run)
    Why overwrite instead of append?
      StatCan periodically publishes revised historical data.
      Append would accumulate duplicate rows for revised periods.
      Overwrite ensures Bronze always mirrors the latest source CSV.
      The audit trail is preserved via _ingested_at + _source_file columns.

    Partition: ingestion_year / ingestion_month
      Enables querying "what did we ingest in a specific month".
      REF_DATE partitioning is deferred to Silver where type casting occurs.
    """
    logger.info(f"  Writing Delta: {delta_path}")

    df_partitioned = df.withColumns({
        "ingestion_year":  F.date_format(F.col("_ingested_at"), "yyyy"),
        "ingestion_month": F.date_format(F.col("_ingested_at"), "MM"),
    })

    (
        df_partitioned.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("ingestion_year", "ingestion_month")
        .save(delta_path)
    )

    # Verify write with read-back count
    spark = SparkSession.getActiveSession()
    written = spark.read.format("delta").load(delta_path).count()
    logger.success(f"  Rows written : {written:,}")

    # Log Delta table version
    from delta.tables import DeltaTable
    version = DeltaTable.forPath(spark, delta_path).history(1).select("version").collect()[0][0]
    logger.success(f"  Delta version: {version}")


def log_bronze_schema(df: DataFrame, table_name: str) -> None:
    """Print final Bronze schema"""
    print(f"\\n  Final Bronze Schema — {table_name}")
    print("  " + "─" * 55)
    for field in df.schema.fields:
        nullable = "nullable" if field.nullable else "NOT NULL"
        print(f"  {field.name:<45} {str(field.dataType):<15} {nullable}")
    print()


# ── Table-level orchestrators ─────────────────────────────────────────────────

def ingest_lfs_province(spark: SparkSession) -> None:
    logger.info("\\n" + "═" * 60)
    logger.info("  BRONZE — lfs_province  (14-10-0287-03)")
    logger.info("═" * 60)
    csv_path = RAW_DATA_PATH / "lfs_province" / "14100287.csv"
    df = read_statcan_csv(spark, csv_path, "lfs_province")
    df = drop_columns(df, PROVINCE_DROP_COLS, "lfs_province")
    df = sanitize_column_names(df)
    df = add_audit_columns(df, source_file="14100287.csv")
    log_bronze_schema(df, "lfs_province")
    write_bronze_delta(df, BRONZE_PROVINCE_PATH, "lfs_province")
    df.unpersist()


def ingest_lfs_industry(spark: SparkSession) -> None:
    logger.info("\\n" + "═" * 60)
    logger.info("  BRONZE — lfs_industry  (14-10-0355-02)")
    logger.info("═" * 60)
    csv_path = RAW_DATA_PATH / "lfs_industry" / "14100355.csv"
    df = read_statcan_csv(spark, csv_path, "lfs_industry")
    df = drop_columns(df, INDUSTRY_DROP_COLS, "lfs_industry")
    df = sanitize_column_names(df)
    df = add_audit_columns(df, source_file="14100355.csv")
    log_bronze_schema(df, "lfs_industry")
    write_bronze_delta(df, BRONZE_INDUSTRY_PATH, "lfs_industry")
    df.unpersist()


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    start_time = datetime.now()

    logger.info("╔" + "═" * 58 + "╗")
    logger.info("║  spark-lakehouse-statcan | Bronze Layer Ingestion       ║")
    logger.info("╚" + "═" * 58 + "╝")
    logger.info(f"  Started      : {start_time}")
    logger.info(f"  Delta base   : {DELTA_BASE_PATH}")

    spark = get_spark("bronze_ingestion")
    logger.info(f"  Spark version: {spark.version}")

    try:
        ingest_lfs_province(spark)
        ingest_lfs_industry(spark)

        elapsed = (datetime.now() - start_time).seconds
        logger.success(f"\\nBronze ingestion complete in {elapsed}s")

    except Exception as e:
        logger.error(f"\\nBronze ingestion failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
