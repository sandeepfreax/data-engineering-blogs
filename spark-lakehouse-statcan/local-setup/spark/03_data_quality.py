"""
Silver-layer data quality gate for spark-lakehouse-statcan.

Runs BEFORE SCD2 and Gold stages. If any check fails beyond its
threshold, the pipeline halts — bad data never reaches the dimension
tables or Gold mart.

CHECKS PERFORMED
────────────────
1. Row count assertion     — Silver must have >= Bronze row count
2. Null rate checks        — Key columns must be below null threshold
3. Freshness check         — Latest ref_date must be within N months
4. Referential integrity   — Every province in industry table must exist in province table
5. Duplicate check         — No duplicate (ref_date, geo, value) rows
6. Value range check       — Employment values must be positive

USAGE
─────
  Standalone:
    python spark/03_data_quality.py

EXIT CODES
──────────
  0 — All checks passed
  1 — One or more checks failed (pipeline should halt)
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

# ── Environment ───────────────────────────────────────────────────────────────
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(ENV_PATH)

SILVER_PROVINCE_PATH = os.environ["SILVER_PROVINCE_PATH"]
SILVER_INDUSTRY_PATH = os.environ["SILVER_INDUSTRY_PATH"]
BRONZE_PROVINCE_PATH = os.environ["BRONZE_PROVINCE_PATH"]
BRONZE_INDUSTRY_PATH = os.environ["BRONZE_INDUSTRY_PATH"]

# ── Thresholds (tune to your data) ────────────────────────────────────────────
NULL_THRESHOLD   = 0.3   # max 5% nulls allowed on key columns
FRESHNESS_MONTHS = 6      # Silver data must have a row within last 6 months
VALUE_MIN        = 0      # employment values must be >= 0

# ── Logging ───────────────────────────────────────────────────────────────────
logger.remove()
logger.add(sys.stdout, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")


# ── Spark Session ─────────────────────────────────────────────────────────────
def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("data_quality")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.master", "local[*]")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ── Check Result Helpers ───────────────────────────────────────────────────────
def passed(check: str, detail: str = "") -> dict:
    logger.success(f"   PASS  {check}" + (f" — {detail}" if detail else ""))
    return {"check": check, "status": "PASS", "detail": detail}

def failed(check: str, detail: str = "") -> dict:
    logger.error(f"   FAIL  {check}" + (f" — {detail}" if detail else ""))
    return {"check": check, "status": "FAIL", "detail": detail}

def warned(check: str, detail: str = "") -> dict:
    logger.warning(f"  ️  WARN  {check}" + (f" — {detail}" if detail else ""))
    return {"check": check, "status": "WARN", "detail": detail}


# ── Individual Checks ─────────────────────────────────────────────────────────

def check_row_counts(spark: SparkSession) -> list:
    """Silver must have >= Bronze row count for each table."""
    results = []
    pairs = [
        ("Province", BRONZE_PROVINCE_PATH, SILVER_PROVINCE_PATH),
        ("Industry", BRONZE_INDUSTRY_PATH, SILVER_INDUSTRY_PATH),
    ]
    for label, bronze_path, silver_path in pairs:
        check_name = f"Row Count [{label}]"
        try:
            bronze_count = spark.read.csv(bronze_path, header=True).count()
            silver_count = spark.read.format("delta").load(silver_path).count()
            detail = f"Bronze={bronze_count:,}  Silver={silver_count:,}"
            if silver_count >= bronze_count:
                results.append(passed(check_name, detail))
            else:
                drop = bronze_count - silver_count
                results.append(failed(check_name, f"{detail}  — DROPPED {drop:,} rows"))
        except Exception as e:
            results.append(failed(check_name, str(e)))
    return results


def check_null_rates(spark: SparkSession) -> list:
    """Key columns must have null rate below NULL_THRESHOLD."""
    results = []
    tables = [
        {"label": "Province Silver", "path": SILVER_PROVINCE_PATH,
         "columns": ["ref_date", "geo", "value"]},
        {"label": "Industry Silver", "path": SILVER_INDUSTRY_PATH,
         "columns": ["ref_date", "naics", "value"]},
    ]
    for table in tables:
        try:
            df = spark.read.format("delta").load(table["path"])
            total = df.count()
            if total == 0:
                results.append(failed(f"Null Rate [{table['label']}]", "Table is empty"))
                continue
            for col in table["columns"]:
                check_name = f"Null Rate [{table['label']}] [{col}]"
                if col not in df.columns:
                    results.append(warned(check_name, f"Column '{col}' not found — skipping"))
                    continue
                null_count = df.filter(F.col(col).isNull()).count()
                rate  = null_count / total
                detail = f"{null_count:,}/{total:,} nulls ({rate:.1%})"
                if rate <= NULL_THRESHOLD:
                    results.append(passed(check_name, detail))
                else:
                    results.append(failed(check_name,
                        f"{detail} — exceeds threshold of {NULL_THRESHOLD:.0%}"))
        except Exception as e:
            results.append(failed(f"Null Rate [{table['label']}]", str(e)))
    return results


def check_freshness(spark: SparkSession) -> list:
    """Latest ref_date in Silver must be within FRESHNESS_MONTHS."""
    results = []
    tables = [
        ("Province Silver", SILVER_PROVINCE_PATH),
        ("Industry Silver",  SILVER_INDUSTRY_PATH),
    ]
    cutoff = datetime.now() - timedelta(days=FRESHNESS_MONTHS * 30)
    for label, path in tables:
        check_name = f"Freshness [{label}]"
        try:
            df = spark.read.format("delta").load(path)
            if "ref_date" not in df.columns:
                results.append(warned(check_name, "ref_date column not found"))
                continue
            latest_row = df.agg(F.max("ref_date").alias("latest")).collect()[0]
            latest = latest_row["latest"]
            if latest is None:
                results.append(failed(check_name, "ref_date is all null"))
                continue
            if isinstance(latest, str):
                latest_dt = datetime.strptime(latest[:10], "%Y-%m-%d")
            else:
                latest_dt = datetime(latest.year, latest.month, latest.day)
            detail = (f"Latest ref_date = {latest_dt.strftime('%Y-%m-%d')}  "
                      f"(cutoff: {cutoff.strftime('%Y-%m-%d')})")
            if latest_dt >= cutoff:
                results.append(passed(check_name, detail))
            else:
                results.append(failed(check_name, f"{detail} — data is STALE"))
        except Exception as e:
            results.append(failed(check_name, str(e)))
    return results


def check_referential_integrity(spark: SparkSession) -> list:
    """Every province code in Industry Silver must exist in Province Silver."""
    check_name = "Referential Integrity [Industry.geo ⊆ Province.geo]"
    try:
        province_df = spark.read.format("delta").load(SILVER_PROVINCE_PATH)
        industry_df = spark.read.format("delta").load(SILVER_INDUSTRY_PATH)
        if "geo" not in province_df.columns or "geo" not in industry_df.columns:
            return [warned(check_name, "geo column not found in one or both tables")]
        valid_geos = province_df.select("geo").distinct()
        orphan_geos = (industry_df.select("geo").distinct()
                        .join(valid_geos, on="geo", how="left_anti"))
        orphan_count = orphan_geos.count()
        if orphan_count == 0:
            return [passed(check_name, "All industry geo codes found in province table")]
        else:
            orphan_vals = [r["geo"] for r in orphan_geos.limit(5).collect()]
            return [failed(check_name,
                f"{orphan_count} orphan geo code(s) — e.g. {orphan_vals}")]
    except Exception as e:
        return [failed(check_name, str(e))]


def check_duplicates(spark: SparkSession) -> list:
    """No duplicate key rows in either Silver table."""
    results = []
    tables = [
        ("Province Silver", SILVER_PROVINCE_PATH, ["ref_date", "geo", "labour_force_characteristics", "sex", "age_group"]),
        ("Industry Silver",  SILVER_INDUSTRY_PATH, ["ref_date", "naics", "labour_force_characteristics"]),
    ]
    for label, path, key_cols in tables:
        check_name = f"Duplicates [{label}]"
        try:
            df = spark.read.format("delta").load(path)
            existing_cols = [c for c in key_cols if c in df.columns]
            if len(existing_cols) < len(key_cols):
                missing = set(key_cols) - set(existing_cols)
                results.append(warned(check_name, f"Columns not found: {missing}"))
                continue
            total = df.count()
            deduped = df.dropDuplicates(existing_cols).count()
            dups = total - deduped
            if dups == 0:
                results.append(passed(check_name, "No duplicates found"))
            else:
                results.append(failed(check_name,
                    f"{dups:,} duplicate rows (key: {existing_cols})"))
        except Exception as e:
            results.append(failed(check_name, str(e)))
    return results


def check_value_ranges(spark: SparkSession) -> list:
    """Employment value column must be >= VALUE_MIN (no negative employment)."""
    results = []
    tables = [
        ("Province Silver", SILVER_PROVINCE_PATH),
        ("Industry Silver",  SILVER_INDUSTRY_PATH),
    ]
    for label, path in tables:
        check_name = f"Value Range [{label}] [value >= {VALUE_MIN}]"
        try:
            df = spark.read.format("delta").load(path)
            if "value" not in df.columns:
                results.append(warned(check_name, "value column not found"))
                continue
            invalid = df.filter(F.col("value") < VALUE_MIN).count()
            if invalid == 0:
                results.append(passed(check_name, "All values within range"))
            else:
                results.append(failed(check_name,
                    f"{invalid:,} rows with value < {VALUE_MIN}"))
        except Exception as e:
            results.append(failed(check_name, str(e)))
    return results


# ── Summary Printer ───────────────────────────────────────────────────────────
def print_summary(results: list) -> int:
    total = len(results)
    passes = sum(1 for r in results if r["status"] == "PASS")
    warns = sum(1 for r in results if r["status"] == "WARN")
    failures = sum(1 for r in results if r["status"] == "FAIL")

    logger.info("\n" + "=" * 60)
    logger.info("  DATA QUALITY SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Total checks : {total}")
    logger.info(f"   Passed    : {passes}")
    logger.info(f"  ️  Warnings  : {warns}")
    logger.info(f"   Failed    : {failures}")
    logger.info("=" * 60)

    if failures > 0:
        logger.error("  DATA QUALITY GATE — FAILED ")
        logger.error("  Pipeline halted. Fix Silver data before proceeding.")
        return 1
    elif warns > 0:
        logger.warning("  DATA QUALITY GATE — PASSED WITH WARNINGS ️")
        return 0
    else:
        logger.success("  DATA QUALITY GATE — ALL CHECKS PASSED ")
        return 0


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("  DATA QUALITY CHECK — Silver Layer")
    logger.info(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    all_results = []

    logger.info("\n── 1. Row Count Assertions ──────────────────────────────")
    all_results += check_row_counts(spark)

    logger.info("\n── 2. Null Rate Checks ──────────────────────────────────")
    all_results += check_null_rates(spark)

    logger.info("\n── 3. Freshness Checks ──────────────────────────────────")
    all_results += check_freshness(spark)

    logger.info("\n── 4. Referential Integrity ─────────────────────────────")
    all_results += check_referential_integrity(spark)

    logger.info("\n── 5. Duplicate Checks ──────────────────────────────────")
    all_results += check_duplicates(spark)

    logger.info("\n── 6. Value Range Checks ────────────────────────────────")
    all_results += check_value_ranges(spark)

    spark.stop()
    exit_code = print_summary(all_results)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
