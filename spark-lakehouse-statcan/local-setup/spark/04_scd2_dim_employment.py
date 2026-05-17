"""
Builds and maintains a SCD Type 2 dimension table: dim_employment_status

WHY SCD TYPE 2 HERE (not in dbt):
  - Delta Lake's MERGE INTO is the native, atomic way to implement SCD2 in Spark
  - dbt's snapshot feature uses a similar pattern but abstracts away the MERGE, hiding the mechanics
  - Doing it in PySpark/Delta gives you full control and is the production pattern

DIMENSION DESIGN:
  Grain     : one row per (geo, labour_force_characteristic, ref_date) version
  Natural key : geo + labour_force_characteristic
  Surrogate key : dim_employment_sk (SHA2 hash of natural key + effective_date)
  SCD2 columns:
    effective_date  — date this record became active (= ref_date of the source row)
    end_date        — date this record was superseded (NULL = current record)
    is_current      — boolean flag for fast current-record filtering

SCD2 MERGE LOGIC (3-way):
  MATCHED + value changed   → expire old row (set end_date, is_current=False)
                              insert new row  (effective_date=today, is_current=True)
  MATCHED + value unchanged → no-op (leave existing row untouched)
  NOT MATCHED               → insert new row as current

IDEMPOTENCY:
  Safe to re-run. Delta MERGE is atomic — partial failures leave the table in its previous consistent state. Re-running produces the same result.

RUN ORDER:
  python spark/01_bronze_ingestion.py
  python spark/02_silver_transform.py
  python spark/04_scd2_dim_employment.py   ← this script
"""

import os
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ── Project paths ────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

SILVER_PROVINCE_PATH = os.getenv("SILVER_PROVINCE_PATH")
DIM_EMPLOYMENT_PATH  = os.getenv("DIM_EMPLOYMENT_PATH")

if not DIM_EMPLOYMENT_PATH:
    raise EnvironmentError(
        "DIM_EMPLOYMENT_PATH not set in .env\\n"
        "Add: DIM_EMPLOYMENT_PATH=/your/project/local-setup/delta_tables/gold/dim_employment_status"
    )

# ── Business keys and tracked attributes ─────────────────────────────────────
NATURAL_KEYS   = ["geo", "labour_force_characteristic"]
TRACKED_ATTRS  = ["value", "status", "data_type"]


# ── Spark session ─────────────────────────────────────────────────────────────
def get_spark() -> SparkSession:
    warehouse_dir  = str(PROJECT_ROOT / "spark-warehouse")
    metastore_path = str(PROJECT_ROOT / "spark-warehouse" / "metastore_db")

    spark = (
        SparkSession.builder
        .appName("scd2_dim_employment")
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


# ── Step 1: Load latest snapshot from Silver ──────────────────────────────────
def load_latest_snapshot(spark: SparkSession) -> DataFrame:
    """
    Load the most recent ref_date record per natural key from Silver.

    FILTER NOTES:
      - data_type = 'Seasonally adjusted': standard economic adjustment
      - statistic_type = 'Estimate': exclude standard error rows
      - is_suppressed: source uses NULL for 'not suppressed', true for suppressed
      coalesce(is_suppressed, false) safely treats NULL as not-suppressed
    """
    logger.info("Loading latest snapshot from Silver province table...")

    df = (
        spark.read.format("delta")
        .load(SILVER_PROVINCE_PATH)
        .filter(
            (F.col("data_type") == "Seasonally adjusted") &
            (F.col("statistic_type") == "Estimate") &
            # FIX: coalesce treats NULL as False (not suppressed)
            (F.coalesce(F.col("is_suppressed"), F.lit(False)) == False)
        )
    )

    # Window: get the single most recent ref_date row per natural key
    window_spec = Window.partitionBy(*NATURAL_KEYS).orderBy(F.col("ref_date").desc())

    latest_df = (
        df.withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    count = latest_df.count()
    logger.info(f"  Latest snapshot: {count:,} rows (one per natural key)")
    return latest_df


# ── Step 2: Prepare incoming batch with SCD2 metadata ────────────────────────
def prepare_incoming(df: DataFrame) -> DataFrame:
    """
    Add SCD2 metadata columns to the incoming snapshot.

    Surrogate key: SHA2 hash of (natural_key | effective_date)
    Deterministic + unique per version — same inputs always produce the same key.
    """
    return (
        df.withColumn(
            "dim_employment_sk",
            F.sha2(F.concat_ws("|", *[F.col(k) for k in NATURAL_KEYS], F.current_date().cast("string")),256))
        .withColumn("effective_date", F.current_date())
        .withColumn("end_date", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .select(
            "dim_employment_sk",
            *NATURAL_KEYS,
            *TRACKED_ATTRS,
            "ref_date",
            "unit_of_measure",
            "scalar_factor",
            "effective_date",
            "end_date",
            "is_current"
        )
    )


# ── Step 3: Initialise dimension table on first run ───────────────────────────
def initialise_dim(incoming_df: DataFrame) -> None:
    """
    Write the full incoming snapshot as the initial dimension table. Only called when the Delta table does not yet exist.
    """
    logger.info(f"  Initialising dimension table at: {DIM_EMPLOYMENT_PATH}")
    (
        incoming_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("is_current")
        .save(DIM_EMPLOYMENT_PATH)
    )
    count = incoming_df.count()
    logger.success(f"  Initialised with {count:,} current records")


# ── Step 4: SCD2 MERGE (stage-then-merge pattern) ────────────────────────────
def run_scd2_merge(spark: SparkSession, incoming_df: DataFrame) -> dict:
    """
    SCD Type 2 MERGE using two-operation pattern:
      Op 1 — MERGE:  expire old rows (whenMatchedUpdate only)
      Op 2 — WRITE:  append new versions + new entities directly

    WHY two operations:
      Delta MERGE evaluates all match conditions against the ORIGINAL table state in one atomic pass. A changed entity
      always matches the existing current row (same natural key, is_current=true in original state), so
      whenNotMatchedInsert never fires for it — the new version row is seen as MATCHED, not unmatched.

    WHY new_versions is derived from changed_join (not a second join):
      Joining incoming_df with a separately cached changed_keys DataFrame can silently return zero rows due to Spark's
      lazy evaluation — the two DataFrames may resolve against different plan snapshots.
      Deriving new_versions from the SAME join that detected the change guarantees both always see the same data.
    """
    dim_table  = DeltaTable.forPath(spark, DIM_EMPLOYMENT_PATH)
    current_df = dim_table.toDF().filter("is_current = true").cache()
    before_count = dim_table.toDF().count()

    # ── Step A+B: Detect changes AND capture new version rows in one join ──
    changed_join = (incoming_df.alias("incoming")
        .join(current_df.alias("current"), NATURAL_KEYS, "inner")
        .filter(" OR ".join([f"incoming.{c} != current.{c}" for c in TRACKED_ATTRS])))
    changed_count = changed_join.count()
    logger.info(f"  Changed keys detected: {changed_count:,}")

    # new_versions = incoming side of the changed join (already filtered to changed rows)
    new_versions = changed_join.select(
        [F.col(f"incoming.{c}").alias(c) for c in incoming_df.columns])

    # ── Step C: Brand-new entities (natural key absent from dim entirely) ──
    all_existing_keys = dim_table.toDF().select(*NATURAL_KEYS).distinct()
    new_entities      = incoming_df.join(all_existing_keys, NATURAL_KEYS, "left_anti")
    new_entity_count  = new_entities.count()
    logger.info(f"  New entities detected: {new_entity_count:,}")

    to_insert    = new_versions.union(new_entities)
    insert_count = to_insert.count()
    logger.info(f"  Rows to insert: {insert_count:,}")

    if changed_count == 0 and insert_count == 0:
        logger.info("  No changes or new entities — no-op")
    else:
        # ── Op 1: MERGE — expire old current rows for changed keys ────────
        if changed_count > 0:
            match_condition = (
                " AND ".join([f"t.{k} = s.{k}" for k in NATURAL_KEYS])
                + " AND t.is_current = true")
            expire_condition = " OR ".join(
                [f"t.{c} != s.{c}" for c in TRACKED_ATTRS])
            (
                dim_table.alias("t")
                .merge(incoming_df.alias("s"), match_condition)
                .whenMatchedUpdate(
                    condition=expire_condition,
                    set={
                        "t.end_date": "date_sub(current_date(), 1)",
                        "t.is_current": "false"
                    }).execute()
            )
            logger.info(f"  Op 1 complete: expired {changed_count:,} old rows")

        # ── Op 2: WRITE — append new versions + new entities directly ─────
        # (cannot use whenNotMatchedInsert - Delta MERGE evaluates against
        #  original table state, so changed rows always appear as MATCHED)
        if insert_count > 0:
            (
                to_insert.write
                .format("delta")
                .mode("append")
                .save(DIM_EMPLOYMENT_PATH)
            )
            logger.info(f"  Op 2 complete: inserted {insert_count:,} new rows")

    current_df.unpersist()

    after_count   = dim_table.toDF().count()
    current_count = dim_table.toDF().filter("is_current = true").count()
    history_count = dim_table.toDF().filter("is_current = false").count()

    return {
        "before":             before_count,
        "after":              after_count,
        "current_records":    current_count,
        "historical_records": history_count,
        "net_change":         after_count - before_count,
        "changed_keys":       changed_count,
        "new_entities":       new_entity_count
    }


# ── Step 5: Verify output ─────────────────────────────────────────────────────
def verify_dimension(spark: SparkSession) -> None:
    """
    Post-merge correctness assertions.

    1. No duplicate current records per natural key
    2. All is_current=True rows have NULL end_date
    3. All is_current=False rows have non-NULL end_date
    """
    logger.info("  Running post-merge dimension quality checks...")
    dim_df = spark.read.format("delta").load(DIM_EMPLOYMENT_PATH)

    # Check 1: no duplicate current records
    duplicates = (
        dim_df.filter("is_current = true")
        .groupBy(*NATURAL_KEYS)
        .count()
        .filter("count > 1")
        .count()
    )
    assert duplicates == 0, f"FAIL: {duplicates} natural keys have >1 current record"
    logger.success("   Check 1 passed: no duplicate current records")

    # Check 2: current rows must have NULL end_date
    bad_current = dim_df.filter("is_current = true AND end_date IS NOT NULL").count()
    assert bad_current == 0, f"FAIL: {bad_current} current rows have non-null end_date"
    logger.success("   Check 2 passed: all current rows have NULL end_date")

    # Check 3: historical rows must have non-NULL end_date
    bad_history = dim_df.filter("is_current = false AND end_date IS NULL").count()
    assert bad_history == 0, f"FAIL: {bad_history} historical rows have NULL end_date"
    logger.success("   Check 3 passed: all historical rows have non-NULL end_date")

    # Sample output
    logger.info("\\n  Sample dimension records:")
    (
        dim_df
        .orderBy("geo", "labour_force_characteristic", "effective_date")
        .select("geo", "labour_force_characteristic", "value",
                "effective_date", "end_date", "is_current")
        .limit(10)
        .show(truncate=False)
    )


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    from datetime import datetime
    start = datetime.now()

    logger.info("=" * 60)
    logger.info("  SCD TYPE 2 — dim_employment_status")
    logger.info("=" * 60)
    logger.info(f"  Output path: {DIM_EMPLOYMENT_PATH}")

    spark = get_spark()
    incoming_df = None

    try:
        # Load + prepare incoming snapshot
        snapshot_df  = load_latest_snapshot(spark)
        incoming_df  = prepare_incoming(snapshot_df)
        incoming_df.cache()

        # Check if dimension table already exists
        dim_path = Path(DIM_EMPLOYMENT_PATH)
        delta_log_exists = (dim_path / "_delta_log").exists()

        if not delta_log_exists:
            logger.info("\\nFirst run — initialising dimension table...")
            initialise_dim(incoming_df)
            total = incoming_df.count()
            stats = {
                "before": 0, "after": total,
                "current_records": total, "historical_records": 0,
                "net_change": total, "changed_keys": 0, "new_entities": total
            }
        else:
            logger.info("\\nExisting dimension found — running SCD2 MERGE...")
            stats = run_scd2_merge(spark, incoming_df)

        # Verify
        logger.info("\\nRunning dimension quality checks...")
        verify_dimension(spark)

        # Audit log
        elapsed = (datetime.now() - start).seconds
        logger.success("\\n" + "=" * 60)
        logger.success("  SCD2 MERGE COMPLETE")
        logger.success("=" * 60)
        logger.info(f"  Records before merge  : {stats['before']:>10,}")
        logger.info(f"  Records after merge   : {stats['after']:>10,}")
        logger.info(f"  Net new rows          : {stats['net_change']:>10,}")
        logger.info(f"  Current records       : {stats['current_records']:>10,}")
        logger.info(f"  Historical records    : {stats['historical_records']:>10,}")
        logger.info(f"  Changed keys          : {stats['changed_keys']:>10,}")
        logger.info(f"  New entities          : {stats['new_entities']:>10,}")
        logger.info(f"  Elapsed               : {elapsed}s")
#        logger.success("\\n  To test SCD2 history accumulation:")
#        logger.success("  1. Modify a value in Silver for one geo+characteristic")
#        logger.success("  2. Re-run this script")
#        logger.success("  3. Query: SELECT * FROM dim WHERE is_current=false LIMIT 5")
        logger.info("=" * 60)
        logger.info("Now run : `dbt build --select gold` - to generate gold dimensions and facts with DBT")

    except Exception as e:
        logger.error(f"\\nSCD2 failed: {e}")
        raise
    finally:
        if incoming_df is not None:
            incoming_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()