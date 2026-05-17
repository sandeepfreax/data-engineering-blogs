"""
Builds mart_employment_trends — a Gold-layer analytical mart enriched with:
  - Month-over-month (MoM) change
  - Year-over-year (YoY) change
  - dim_employment_sk join (SCD2 point-in-time surrogate key)
  - Rolling 3-month average

WHY THIS SCRIPT EXISTS (not in dbt):
  dbt's fct_employment_monthly does the SQL-friendly transforms well.
  This script does what dbt cannot do efficiently:
    1. Window functions over 179k rows with LAG(12) — Spark parallelizes this across partitions; dbt-spark runs it as a
        single SQL pass
    2. SCD2 point-in-time join — joining fact rows to the dim version that was ACTIVE at ref_date
        (effective_date <= ref_date < end_date).
    3. Written to a separate Delta path (not spark-warehouse) so it survives dbt full-refresh runs

GRAIN:
  One row per (ref_date, geo, labour_force_characteristic, gender, age_group)
  Same grain as fct_employment_monthly — this is an enriched version of it

OUTPUT:
  local-setup/delta_tables/gold/mart_employment_trends
"""

import os
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

FCT_PATH         = str(PROJECT_ROOT / "spark-warehouse" / "fct_employment_monthly")
DIM_EMPLOYMENT_PATH = os.getenv("DIM_EMPLOYMENT_PATH")
DIM_GEO_PATH     = str(PROJECT_ROOT / "spark-warehouse" / "dim_geo")
MART_PATH        = os.getenv(
    "MART_EMPLOYMENT_PATH",
    str(PROJECT_ROOT / "delta_tables" / "gold" / "mart_employment_trends")
)

# Natural keys for SCD2 join
NATURAL_KEYS = ["geo", "labour_force_characteristic"]


# ── Spark session ─────────────────────────────────────────────────────────────
def get_spark() -> SparkSession:
    warehouse_dir  = str(PROJECT_ROOT / "spark-warehouse")
    metastore_path = str(PROJECT_ROOT / "spark-warehouse" / "metastore_db")

    spark = (
        SparkSession.builder
        .appName("gold_employment_mart")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={metastore_path};create=true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── Step 1: Load fact table ───────────────────────────────────────────────────
def load_fact(spark: SparkSession) -> DataFrame:
    """
    Load fct_employment_monthly from dbt-built Delta table.
    Filter to Total Gender only for clean MoM/YoY trends — gender splits are kept in the source fact for ad-hoc queries.
    """
    logger.info("Loading fct_employment_monthly...")
    df = (
        spark.read.format("delta").load(FCT_PATH)
        .filter(F.col("gender") == "Total - Gender")
    )
    count = df.count()
    logger.info(f"  Fact rows (Total Gender): {count:,}")
    return df


# ── Step 2: Add window-function metrics ──────────────────────────────────────
def add_trend_metrics(df: DataFrame) -> DataFrame:
    """
    Add MoM, YoY, and rolling 3-month average using Spark window functions.

    WHY window functions here (not dbt):
      LAG(12) over a partition of 179k rows with ORDER BY ref_date is a distributed operation — Spark assigns one
      partition per (geo, labour_force_characteristic) group and processes them in parallel. dbt-spark runs this as a
      single-threaded SQL pass.

    WINDOW SPEC:
      Partition by natural key — each entity's time series is independent.
      Order by ref_date — lag looks back N months within that entity.

    MoM  = current value - previous month value
    YoY  = current value - same month last year (lag 12 months)
    MA3  = average of current + 2 previous months (smooths seasonal noise)

    NULL handling:
      First row of each partition has no lag → MoM = NULL (correct)
      First 12 rows have no YoY → YoY = NULL (correct)
      These NULLs are meaningful — don't coalesce to 0
    """
    logger.info("Computing trend metrics (MoM, YoY, MA3)...")

    window_entity = (
        Window
        .partitionBy(*NATURAL_KEYS)
        .orderBy("ref_date")
    )

    # 3-month rolling window (current row + 2 preceding)
    window_rolling = (
        Window
        .partitionBy(*NATURAL_KEYS)
        .orderBy("ref_date")
        .rowsBetween(-2, 0)
    )

    enriched = (
        df
        # MoM: value vs previous month
        .withColumn(
            "mom_change_thousands",
            F.round( F.col("value_thousands") - F.lag("value_thousands", 1).over(window_entity), 1))
        .withColumn(
            "mom_change_pct",
            F.round(
                (F.col("value_thousands") - F.lag("value_thousands", 1).over(window_entity))
                / F.lag("value_thousands", 1).over(window_entity) * 100, 2))
        # YoY: value vs same month last year
        .withColumn(
            "yoy_change_thousands",
            F.round(F.col("value_thousands") - F.lag("value_thousands", 12).over(window_entity), 1))
        .withColumn(
            "yoy_change_pct",
            F.round(
                (F.col("value_thousands") - F.lag("value_thousands", 12).over(window_entity))
                / F.lag("value_thousands", 12).over(window_entity) * 100, 2))
        # 3-month rolling average (smooths seasonal noise)
        .withColumn(
            "rolling_3m_avg_thousands",
            F.round(F.avg("value_thousands").over(window_rolling), 1))
    )

    logger.info("  Trend metrics computed")
    return enriched


# ── Step 3: SCD2 point-in-time join ─────────────────────────────────────────
def join_scd2_dim(spark: SparkSession, fact_df: DataFrame) -> DataFrame:
    """
    Join fact rows to dim_employment_status using point-in-time SCD2 logic.

    POINT-IN-TIME JOIN (the hard part):
      A standard equi-join on natural key would always return the CURRENT dim row — ignoring historical versions.
      That's wrong for a slowly changing dimension.

      Correct logic: for each fact row at ref_date, find the dim version that was ACTIVE at that point in time:
        effective_date <= ref_date < end_date   (for expired rows)
        effective_date <= ref_date AND is_current = true  (for current row)

      We use a broadcast hint on the dim (99 rows) to make it fast: Spark sends the entire small dim table to every
      executor, eliminating shuffle.

    WHY THIS MATTERS:
      If Alberta's Unemployment rate changed from 5.4% to 7.2% in May 2026, fact rows from Jan 2026 should join to the
      5.4% dim version, not the current 7.2% version. This is the entire point of SCD2.

    NOTE: For this dataset, dim_employment_status was initialised today (May 2026), so most historical fact rows will
        not match any dim version (dim only covers from effective_date onwards). In production, you would back-load the
        dim from historical Silver data. The join logic is correct regardless — NULLs on dim_employment_sk
        indicate pre-dim-history rows.
    """
    logger.info("Joining to SCD2 dim_employment_status (point-in-time)...")

    dim_df = (
        spark.read.format("delta").load(DIM_EMPLOYMENT_PATH)
        .select(
            "dim_employment_sk",
            *NATURAL_KEYS,
            "effective_date",
            "end_date",
            "is_current"
        )
        .hint("broadcast")   # dim is tiny (99 rows) — broadcast to all executors
    )

    # Point-in-time join condition: natural keys match AND fact ref_date falls within dim version's date range
    joined = (
        fact_df.alias("f")
        .join(
            dim_df.alias("d"),
            on=(
                (F.col("f.geo") == F.col("d.geo")) &
                (F.col("f.labour_force_characteristic") == F.col("d.labour_force_characteristic")) &
                (F.col("f.ref_date") >= F.col("d.effective_date")) &
                (F.col("d.is_current") | (F.col("f.ref_date") < F.col("d.end_date")))),
            how="left"
        )
        .select(
            # Surrogate key from SCD2 dim (NULL for pre-history rows)
            F.col("d.dim_employment_sk"),
            # All fact columns
            F.col("f.*"),)
    )

    matched   = joined.filter(F.col("dim_employment_sk").isNotNull()).count()
    unmatched = joined.filter(F.col("dim_employment_sk").isNull()).count()
    logger.info(f"  SCD2 join: {matched:,} matched | {unmatched:,} pre-history (NULL sk)")

    return joined


# ── Step 4: Join dim_geo ──────────────────────────────────────────────────────
def join_dim_geo(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Enrich with geo dimension for geo_level and geo_region columns.
    Broadcast hint: dim_geo has only 11 rows.
    """
    logger.info("Joining dim_geo...")
    geo_df = (
        spark.read.format("delta").load(DIM_GEO_PATH)
        .select("geo_key", "geo_name", "geo_level", "geo_region")
        .hint("broadcast"))

    return (
        df.join(
            geo_df,
            df.geo == geo_df.geo_name,
            "left")
        .drop("geo_name")
    )


# ── Step 5: Write mart ────────────────────────────────────────────────────────
def write_mart(df: DataFrame) -> None:
    """
    Write final mart as Delta, partitioned by ref_year for efficient time-range queries (e.g. dashboard filtering by year).
    """
    logger.info(f"Writing mart to: {MART_PATH}")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("ref_year")
        .save(MART_PATH)
    )
    logger.success("  Mart written successfully")

# ── Step 6: Register in HMS for DBT test cases ─────────────────────────────────
def register_in_metastore(spark: SparkSession) -> None:
    """
    Register mart as a Hive-managed table so dbt can query it via SQL.
    Uses CREATE TABLE IF NOT EXISTS ... LOCATION so the Delta files on disk are exposed to the Spark SQL catalog
    without moving any data.
    DROP first handles schema evolution across re-runs.
    """
    logger.info("Registering mart in Hive metastore...")
    spark.sql("DROP TABLE IF EXISTS mart_employment_trends")
    spark.sql(f"""
        CREATE TABLE mart_employment_trends
        USING DELTA
        LOCATION '{MART_PATH}'
    """)
    count = spark.sql("SELECT COUNT(*) FROM mart_employment_trends").first()[0]
    logger.info(f"  Registered: mart_employment_trends ({count:,} rows visible to dbt)")

# ── Step 7: Verify ────────────────────────────────────────────────────────────
def verify_mart(spark: SparkSession) -> None:
    """Post-write quality checks on the mart."""
    logger.info("Running mart quality checks...")
    df = spark.read.format("delta").load(MART_PATH)

    total = df.count()
    nulls_mom = df.filter(F.col("mom_change_thousands").isNull() &
                              (F.col("ref_date") > df.select(F.min("ref_date")).first()[0])
                             ).count()
    has_yoy = df.filter(F.col("yoy_change_thousands").isNotNull()).count()
    has_geo_key = df.filter(F.col("geo_key").isNotNull()).count()

    logger.success(f"   Total mart rows      : {total:,}")
    logger.success(f"   Rows with YoY data   : {has_yoy:,}")
    logger.success(f"   Rows with geo_key    : {has_geo_key:,}")
    logger.info(   f"   NULL MoM (non-first) : {nulls_mom:,} (should be 0)")

    logger.info("\n  Sample mart rows (Alberta Employment, recent):")
    (
        df.filter((F.col("geo") == "Alberta") & (F.col("labour_force_characteristic") == "Employment"))
        .orderBy(F.col("ref_date").desc())
        .select( "ref_date", "value_thousands", "mom_change_thousands", "mom_change_pct", "yoy_change_thousands",
                 "yoy_change_pct", "rolling_3m_avg_thousands", "geo_region")
        .limit(5)
        .show(truncate=False)
    )

    dupes = (
        df.groupBy("ref_date", "geo", "labour_force_characteristic")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    assert dupes == 0, f"FAIL: {dupes} duplicate rows found in mart"
    logger.success(f"   No duplicate rows (grain integrity confirmed)")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    from datetime import datetime
    start = datetime.now()

    logger.info("=" * 60)
    logger.info("  GOLD MART — mart_employment_trends")
    logger.info("=" * 60)

    spark = get_spark()

    try:
        fact_df    = load_fact(spark)
        trend_df   = add_trend_metrics(fact_df)
        scd2_df    = join_scd2_dim(spark, trend_df)
        final_df   = join_dim_geo(spark, scd2_df)

        write_mart(final_df)
        register_in_metastore(spark)
        verify_mart(spark)

        elapsed = (datetime.now() - start).seconds
        logger.success("\\n" + "=" * 60)
        logger.success("  MART BUILD COMPLETE")
        logger.success("=" * 60)
        logger.info(f"  Output : {MART_PATH}")
        logger.info(f"  Elapsed: {elapsed}s")

    except Exception as e:
        logger.error(f"Mart build failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()