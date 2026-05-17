"""
End-to-end pipeline orchestrator for spark-lakehouse-statcan.

Runs the full medallion pipeline in order:
  Bronze → Silver → SCD2 Gold Dim → Gold Mart → dbt build

WHY A DEDICATED RUNNER:
  - Single entry point for local dev, CI, and AWS EMR scheduled runs
  - Captures per-stage timing for performance monitoring
  - Fails fast: if any stage fails, downstream stages are skipped
  - Idempotent: safe to re-run at any time

USAGE:
  # Full pipeline
  python spark/06_pipeline_runner.py

  # Skip bronze ingestion (Silver onwards, when Bronze is already loaded)
  python spark/06_pipeline_runner.py --skip-bronze

  # Run a specific stage only
  python spark/06_pipeline_runner.py --stage silver
  python spark/06_pipeline_runner.py --stage scd2
  python spark/06_pipeline_runner.py --stage dbt_deps
  python spark/06_pipeline_runner.py --stage dbt_gold
  python spark/06_pipeline_runner.py --stage mart
  python spark/06_pipeline_runner.py --stage dbt
"""

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
import os

from loguru import logger

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent / "local-setup"
SPARK_DIR    = PROJECT_ROOT / "spark"
DBT_DIR      = PROJECT_ROOT / "dbt"

# Configure loguru — pipeline-level log goes to file + console
LOG_PATH = PROJECT_ROOT / "logs" / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

logger.remove()
logger.add(sys.stdout, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
logger.add(str(LOG_PATH), level="DEBUG", format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}")


# Load .env into the current process — child subprocesses inherit it automatically
# especially done to run dbt commands from this python file
def load_dotenv(env_path: Path) -> None:
    if not env_path.exists():
        logger.warning(f".env file not found at {env_path}, skipping")
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    logger.info(f"  Loaded env vars from {env_path}")

# Call it before any stage runs — adjust path if your .env is elsewhere
load_dotenv(PROJECT_ROOT / ".env")


# ── Stage definitions ─────────────────────────────────────────────────────────
STAGES = {
    "bronze": {
        "label":   "01 Bronze Ingestion",
        "script":  SPARK_DIR / "01_bronze_ingestion.py",
        "type":    "python",
    },
    "silver": {
        "label":   "02 Silver Transform",
        "script":  SPARK_DIR / "02_silver_transform.py",
        "type":    "python",
    },
    "dq": {
        "label":   "03 Data Quality",
        "script":  SPARK_DIR / "03_data_quality.py",
        "type":    "python",
    },
    "scd2": {
        "label":   "04 SCD2 Dim Employment",
        "script":  SPARK_DIR / "04_scd2_dim_employment.py",
        "type":    "python",
    },
    "dbt_deps": {
        "label":   "05a dbt Deps Install",
        "command": ["dbt", "deps"],
        "cwd":     DBT_DIR,
        "type":    "shell",
    },
    "dbt_gold": {
        "label":   "05b dbt Gold Models",
        "command": ["dbt", "build", "--select", "tag:gold"],
        "cwd":     DBT_DIR,
        "type":    "shell",
    },
    "mart": {
        "label":   "05 Gold Employment Mart",
        "script":  SPARK_DIR / "05_gold_employment_mart.py",
        "type":    "python",
    },
    "dbt": {
        "label":   "06 dbt Build (tests + Gold models)",
        "command": ["dbt", "build"],
        "cwd":     DBT_DIR,
        "type":    "shell",
    },
}

# Full pipeline order — used when no --stage flag is passed
PIPELINE_ORDER = ["bronze", "silver", "dq", "scd2", "dbt_deps", "dbt_gold", "mart", "dbt"]


# ── Stage runner ──────────────────────────────────────────────────────────────
def run_stage(stage_key: str) -> dict:
    """
    Run a single pipeline stage. Returns a result dict with:
      stage, label, status (success/failed/skipped), duration_s, error
    """
    stage    = STAGES[stage_key]
    label    = stage["label"]
    start    = time.time()
    result   = {"stage": stage_key, "label": label, "error": None}

    logger.info(f"{'─' * 55}")
    logger.info(f"  STARTING: {label}")
    logger.info(f"{'─' * 55}")

    try:
        if stage["type"] == "python":
            script = stage["script"]
            if not script.exists():
                raise FileNotFoundError(f"Script not found: {script}")
            cmd = [sys.executable, str(script)]
            cwd = str(PROJECT_ROOT)

        elif stage["type"] == "shell":
            cmd = stage["command"]
            cwd = str(stage.get("cwd", PROJECT_ROOT))

        proc = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=False,
            text=True,
            check=True
        )

        duration = round(time.time() - start, 1)
        result.update({"status": "success", "duration_s": duration})
        logger.success(f"   COMPLETED: {label} ({duration}s)")

    except subprocess.CalledProcessError as e:
        duration = round(time.time() - start, 1)
        result.update({"status": "failed", "duration_s": duration,
                        "error": f"Exit code {e.returncode}"})
        logger.error(f"   FAILED: {label} ({duration}s) — exit code {e.returncode}")

    except FileNotFoundError as e:
        duration = round(time.time() - start, 1)
        result.update({"status": "failed", "duration_s": duration, "error": str(e)})
        logger.error(f"   FAILED: {label} — {e}")

    return result


# ── Pipeline summary ──────────────────────────────────────────────────────────
def print_summary(results: list, total_elapsed: float) -> None:
    logger.info("\n" + "=" * 55)
    logger.info("  PIPELINE SUMMARY")
    logger.info("=" * 55)

    for r in results:
        icon = "" if r["status"] == "success" else (" " if r["status"] == "skipped" else "")
        duration = f"{r['duration_s']}s" if r.get("duration_s") else "—"
        logger.info(f"  {icon}  {r['label']:<35} {duration:>6}")
        if r.get("error"):
            logger.info(f"       Error: {r['error']}")

    logger.info("─" * 55)
    failed = [r for r in results if r["status"] == "failed"]
    if failed:
        logger.error(f"  Pipeline FAILED — {len(failed)} stage(s) failed")
        logger.error(f"  First failure: {failed[0]['label']}")
    else:
        logger.success(f"  Pipeline COMPLETE   Total: {round(total_elapsed, 1)}s")
    logger.info(f"  Log: {LOG_PATH}")
    logger.info("=" * 55)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="spark-lakehouse-statcan pipeline runner"
    )
    parser.add_argument(
        "--skip-bronze",
        action="store_true",
        help="Skip bronze ingestion (use when Bronze is already loaded)"
    )
    parser.add_argument(
        "--stage",
        choices=list(STAGES.keys()),
        help="Run a single stage only"
    )
    args = parser.parse_args()

    pipeline_start = time.time()

    logger.info("=" * 55)
    logger.info("  SPARK LAKEHOUSE — STATCAN PIPELINE")
    logger.info(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 55)

    # Determine which stages to run
    if args.stage:
        stages_to_run = [args.stage]
        logger.info(f"  Mode: single stage ({args.stage})")
    else:
        stages_to_run = PIPELINE_ORDER.copy()
        if args.skip_bronze:
            stages_to_run.remove("bronze")
            logger.info("  Mode: full pipeline (bronze skipped)")
        else:
            logger.info("  Mode: full pipeline")

    results = []

    for stage_key in PIPELINE_ORDER:
        if stage_key not in stages_to_run:
            results.append({
                "stage":      stage_key,
                "label":      STAGES[stage_key]["label"],
                "status":     "skipped",
                "duration_s": 0,
                "error":      None
            })
            logger.info(f"    SKIPPED: {STAGES[stage_key]['label']}")
            continue

        result = run_stage(stage_key)
        results.append(result)

        # Fail fast — stop pipeline on first failure
        if result["status"] == "failed":
            logger.error("  Pipeline halted due to stage failure.")
            remaining = PIPELINE_ORDER[PIPELINE_ORDER.index(stage_key) + 1:]
            for remaining_key in remaining:
                if remaining_key in stages_to_run:
                    results.append({
                        "stage": remaining_key,
                        "label": STAGES[remaining_key]["label"],
                        "status": "skipped",
                        "duration_s": 0,
                        "error": "Skipped due to upstream failure"
                    })
            break

    total_elapsed = time.time() - pipeline_start
    print_summary(results, total_elapsed)

    failed = [r for r in results if r["status"] == "failed"]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()