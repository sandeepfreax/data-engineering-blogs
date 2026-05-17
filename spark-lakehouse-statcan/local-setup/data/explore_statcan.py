"""
StatCan Dataset Loader & Profiler
----------------------------------
Works with manually downloaded CSV files from Statistics Canada.

Expected file layout (already in place):
    data/raw/lfs_province/14100287.csv
    data/raw/lfs_province/14100287_MetaData.csv
    data/raw/lfs_industry/14100355.csv
    data/raw/lfs_industry/14100355_MetaData.csv

Usage:
    python data/explore_statcan.py

What this script does:
    1. Validates all expected files exist
    2. Prints full schema profile for each table
       (column names, data types, sample values, null counts)
    3. Prints metadata summary (frequency, source, notes)
    4. Reports row counts and date ranges
"""

import sys
from pathlib import Path
import pandas as pd
from loguru import logger

# ── Path config ───────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw"

TABLES = {
    "lfs_province": {
        "data_file":     RAW_DATA_PATH / "lfs_province" / "14100287.csv",
        "metadata_file": RAW_DATA_PATH / "lfs_province" / "14100287_MetaData.csv",
        "description":   "LFS — Labour Force Characteristics by Province, Gender & Age Group",
        "table_id":      "14-10-0287-03",
    },
    "lfs_industry": {
        "data_file":     RAW_DATA_PATH / "lfs_industry" / "14100355.csv",
        "metadata_file": RAW_DATA_PATH / "lfs_industry" / "14100355_MetaData.csv",
        "description":   "LFS — Employment by Industry",
        "table_id":      "14-10-0355-02",
    },
}


# ── Validators ────────────────────────────────────────────────────
def validate_files(table_key: str, config: dict) -> bool:
    """Check both data + metadata files exist and are non-empty."""
    all_good = True
    for label, path in [("data", config["data_file"]),
                         ("metadata", config["metadata_file"])]:
        if not path.exists():
            logger.error(f" Missing {label} file: {path}")
            all_good = False
        elif path.stat().st_size == 0:
            logger.error(f" Empty {label} file: {path}")
            all_good = False
        else:
            size_mb = path.stat().st_size / 1_000_000
            logger.success(f" Found {label} file : {path.name}  ({size_mb:.1f} MB)")
    return all_good


# ── Profiler ──────────────────────────────────────────────────────

def profile_table(table_key: str, config: dict) -> pd.DataFrame:
    """
    Full schema profile of a StatCan CSV table.
    Returns the loaded DataFrame for further inspection.
    """
    data_file = config["data_file"]

    logger.info(f"\n  Loading: {data_file.name}")
    # StatCan CSVs use UTF-8 BOM encoding
    df = pd.read_csv(data_file, encoding="utf-8-sig", low_memory=False)

    total_rows = len(df)
    total_cols = len(df.columns)

    # ── Header ────────────────────────────────────────────────────
    print("\n" + "═" * 70)
    print(f"  TABLE   : {config['description']}")
    print(f"  ID      : {config['table_id']}")
    print(f"  File    : {data_file.name}")
    print(f"  Rows    : {total_rows:,}")
    print(f"  Columns : {total_cols}")
    print("═" * 70)

    # ── Column-level profile ──────────────────────────────────────
    print(f"\n  {'#':<4} {'Column Name':<35} {'Dtype':<12} {'Nulls':<8} {'Sample Values'}")
    print("  " + "─" * 80)

    for i, col in enumerate(df.columns, 1):
        dtype     = str(df[col].dtype)
        null_cnt  = df[col].isna().sum()
        null_pct  = (null_cnt / total_rows * 100) if total_rows > 0 else 0
        # Sample: up to 3 unique non-null values
        samples   = df[col].dropna().unique()[:3]
        sample_str = " | ".join(str(s)[:20] for s in samples)

        print(f"  {i:<4} {col:<35} {dtype:<12} "
              f"{null_cnt:>6} ({null_pct:4.1f}%)  {sample_str}")

    # ── Date range ────────────────────────────────────────────────
    date_col = next((c for c in df.columns if "REF_DATE" in c.upper()), None)
    if date_col:
        print(f"\n Date range  : {df[date_col].min()}  →  {df[date_col].max()}")

    # ── GEO / Province distribution ───────────────────────────────
    geo_col = next((c for c in df.columns if "GEO" in c.upper()), None)
    if geo_col:
        geo_counts = df[geo_col].value_counts()
        print(f"\n  Geographies ({len(geo_counts)} unique):")
        for geo, cnt in geo_counts.head(15).items():
            print(f"       {geo:<40} {cnt:>8,} rows")

    # ── Key dimension columns ─────────────────────────────────────
    dim_keywords = ["SEX", "AGE", "LABOUR", "INDUSTRY", "ESTIMATE",
                    "NAICS", "CHARACTERISTIC", "STATUS", "TYPE"]
    for col in df.columns:
        if any(kw in col.upper() for kw in dim_keywords):
            unique_vals = df[col].dropna().unique()
            if len(unique_vals) <= 20:
                print(f"\n  {col} ({len(unique_vals)} unique values):")
                for v in unique_vals:
                    print(f"       • {v}")

    # ── VALUE column stats ────────────────────────────────────────
    val_col = next((c for c in df.columns if c.upper() == "VALUE"), None)
    if val_col:
        numeric_vals = pd.to_numeric(df[val_col], errors="coerce")
        print(f"\n  VALUE column stats:")
        print(f"       Min    : {numeric_vals.min():,.2f}")
        print(f"       Max    : {numeric_vals.max():,.2f}")
        print(f"       Mean   : {numeric_vals.mean():,.2f}")
        print(f"       Nulls  : {numeric_vals.isna().sum():,}  "
              f"(includes StatCan suppression codes like '..' and 'F')")

    print()
    return df


# ── Metadata reader ───────────────────────────────────────────────

def read_metadata(config: dict) -> None:
    """Print key lines from the StatCan _MetaData.csv file."""
    meta_file = config["metadata_file"]

    print(f"\n  Metadata: {meta_file.name}")
    print("  " + "─" * 50)

    try:
        # MetaData CSVs are free-form — read as raw text rows
        meta_df = pd.read_csv(meta_file, encoding="utf-8-sig",
                               header=None, on_bad_lines="skip")
        # Print first 20 meaningful rows
        for _, row in meta_df.head(25).iterrows():
            line = "  ".join(str(v) for v in row if pd.notna(v) and str(v).strip())
            if line.strip():
                print(f"  {line}")
    except Exception as e:
        logger.warning(f"  Could not read metadata: {e}")

    print()


# ── Main ──────────────────────────────────────────────────────────

def main():
    logger.info("=" * 70)
    logger.info("  StatCan LFS — Local File Validator & Schema Profiler")
    logger.info("=" * 70)

    all_valid = True
    loaded_frames = {}

    # ── Step 1: Validate all files exist ─────────────────────────
    logger.info("\nValidating files...")
    for key, config in TABLES.items():
        logger.info(f"\n  [{key}]")
        if not validate_files(key, config):
            all_valid = False

    if not all_valid:
        logger.error("\nMissing files detected. Please ensure all CSV files")
        logger.error(f"   are placed under: {RAW_DATA_PATH}")
        logger.error("   Expected layout:")
        logger.error("     data/raw/lfs_province/14100287.csv")
        logger.error("     data/raw/lfs_province/14100287_MetaData.csv")
        logger.error("     data/raw/lfs_industry/14100355.csv")
        logger.error("     data/raw/lfs_industry/14100355_MetaData.csv")
        sys.exit(1)

    # ── Step 2: Profile each table ────────────────────────────────
    logger.info("\n\nProfiling tables...\n")
    for key, config in TABLES.items():
        df = profile_table(key, config)
        loaded_frames[key] = df
        read_metadata(config)

    # ── Step 3: Cross-table join feasibility check ────────────────
    print("═" * 70)
    print("Cross-Table Join Feasibility Check")
    print("═" * 70)

    province_df  = loaded_frames.get("lfs_province")
    industry_df  = loaded_frames.get("lfs_industry")

    if province_df is not None and industry_df is not None:
        # Check shared date column
        p_dates = set(province_df["REF_DATE"].dropna().unique()) \
            if "REF_DATE" in province_df.columns else set()
        i_dates = set(industry_df["REF_DATE"].dropna().unique()) \
            if "REF_DATE" in industry_df.columns else set()

        overlap = p_dates & i_dates
        print(f"\n  Overlapping REF_DATE periods : {len(overlap):,}")
        if overlap:
            sorted_overlap = sorted(overlap)
            print(f"  Earliest shared date        : {sorted_overlap[0]}")
            print(f"  Latest shared date          : {sorted_overlap[-1]}")
            print(f"\n  Both tables share {len(overlap)} common time periods")
            print(f"     → Safe to join on REF_DATE for Gold layer fact table")
        else:
            print("No overlapping dates found — review date formats")

    print("\n" + "═" * 70)
    logger.success("Profiling complete!")
    print("═" * 70 + "\n")


if __name__ == "__main__":
    main()
