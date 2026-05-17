# spark-lakehouse-statcan

> **A production-grade lakehouse pipeline built on Canadian Labour Force Survey data — running entirely on your laptop.**

This project implements a complete **medallion architecture** (Bronze → Silver → Gold) on real Statistics Canada data, combining Apache Spark, Delta Lake, and dbt into a single orchestrated pipeline. It demonstrates how a modern data lakehouse is designed, built, and run — without a cloud bill.

---

## The Problem This Solves

Canadian employment data from Statistics Canada is published as raw CSV files — split by province and by industry, with no joins, no history, and no analytical model on top of it. This project answers: *what does it take to turn that raw government data into a queryable, historically tracked, analytically ready Gold layer?*

The answer is a full lakehouse stack, built from scratch.

---

## Data Sources

Both tables are sourced from [Statistics Canada](https://www.statcan.gc.ca/):

| Table | ID | Description |
|-------|----|-------------|
| Labour Force Survey — Province | `14-10-0287-03` | Monthly employment by province and demographic |
| Labour Force Survey — Industry | `14-10-0355-02` | Monthly employment by industry and province |

---

## Architecture

```
Raw CSV (StatCan)
      │
      ▼
┌─────────────┐
│   BRONZE    │  01 — Raw ingestion, schema enforcement
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   SILVER    │  02 — Cleaned, typed, standardised
└──────┬──────┘
       │
       ▼
┌─────────────────────┐
│  DATA QUALITY GATE  │  03 — 6 checks, fail-fast, pipeline halts on failure
└──────┬──────────────┘
       │
       ▼
┌──────────────────────────┐
│  SCD Type 2 Dimension    │  04 — dim_employment with full change history
└──────┬───────────────────┘
       │
       ▼
┌─────────────┐     ┌──────────────────────────────┐
│  dbt GOLD   │────▶│  dim_geo · fct_employment     │  04a/04b
└─────────────┘     │  monthly                      │
                    └──────────────────────────────┘
       │
       ▼
┌─────────────┐
│  GOLD MART  │  05 — Final analytical mart, query-ready
└─────────────┘
       │
       ▼
┌─────────────┐
│  dbt build  │  06 — Full model + test run (schema contracts)
└─────────────┘
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Compute | Apache Spark (PySpark) |
| Storage format | Delta Lake |
| Transformation (Gold) | dbt (Spark adapter) |
| Data exploration | Pandas |
| Orchestration | Custom Python pipeline runner |
| Language | Python 3.x |
| Environment | Local — no cloud required |

---

## Key Engineering Decisions

### Why Medallion Architecture?
Each layer has a single responsibility. Bronze preserves raw data exactly as received. Silver enforces quality and consistency. Gold serves analytical queries. This separation makes debugging, reprocessing, and schema evolution manageable at any scale.

### Why Delta Lake locally?
Delta Lake gives ACID transactions, schema enforcement, and time travel on a laptop — the same guarantees you get on Databricks or EMR, without the cloud cost. Every table is replayable from scratch.

### Why SCD Type 2?
The Labour Force Survey captures monthly employment snapshots. SCD2 preserves the full history of how those values changed over time, making it possible to answer questions like *"what was the employment rate in Ontario in March 2022?"* accurately, even after the data has been updated.

### Why dbt for Gold?
dbt handles the declarative SQL transformation layer cleanly — version-controlled models, built-in tests, and lineage out of the box. The Spark adapter means dbt writes directly to `spark-warehouse`, which the Gold Mart Spark script then reads from.

### Why a custom pipeline runner?
A lightweight Python orchestrator (`05_pipeline_runner.py`) was chosen over Airflow or Prefect to keep the local setup dependency-free. It supports `--stage` and `--skip-bronze` flags, fail-fast behaviour, and per-stage timing — enough for local development and a direct map to a Step Functions state machine on AWS EMR.

---

## Pipeline Stages

| #   | Stage               | Script                        | Description                                 |
|-----|---------------------|-------------------------------|---------------------------------------------|
| 01  | Bronze Ingestion    | `01_bronze_ingestion.py`      | Ingest raw StatCan CSVs into Delta Bronze   |
| 02  | Silver Transform    | `02_silver_transform.py`      | Clean, type, and normalise Bronze data      |
| 03  | Data Quality Gate   | `03_data_quality.py`          | Data Quality Check on Bronze and Silver     |
| 04  | SCD2 Dim Employment | `04_scd2_dim_employment.py`   | Merge Silver into SCD Type 2 dimension      |
| 05a | dbt Deps            | `dbt deps`                    | Install dbt package dependencies            |
| 05b | dbt Gold Models     | `dbt build --select tag:gold` | Materialise `dim_geo` and `fct_employment_monthly` |
| 05  | Gold Mart           | `04_gold_employment_mart.py`  | Build analytical Gold mart from dbt outputs |
| 06  | dbt Full Build      | `dbt build`                   | Run all dbt models + tests                  |

---

## Data Quality Gate

`03_data_quality.py` acts as a hard gate between Silver and the dimension/Gold layers.
If any check fails, the pipeline halts before bad data reaches the SCD2 history or Gold mart.

| Check | What It Validates | Failure Behaviour |
|-------|-------------------|-------------------|
| **Row Count** | Silver row count ≥ Bronze row count | ❌ Halts pipeline |
| **Null Rate** | `ref_date`, `geo`/`naics`, `value` — max 5% nulls | ❌ Halts pipeline |
| **Freshness** | Latest `ref_date` within 6 months of run date | ❌ Halts pipeline |
| **Referential Integrity** | Every `industry.geo` exists in `province.geo` | ❌ Halts pipeline |
| **Duplicates** | No duplicate `(ref_date, geo/naics, value)` rows | ❌ Halts pipeline |
| **Value Range** | No negative employment values | ❌ Halts pipeline |

> **Why not just dbt tests?**
> dbt tests validate Gold models *after* they are built — they are a post-build contract check.
> `03_data_quality.py` catches anomalies at Silver *before* they corrupt SCD2 history
> or propagate into the Gold mart. Both layers are intentional and complementary.

## Running the Pipeline

Full instructions are in [`local-setup/README.md`](./local-setup/README.md).

```bash
# Full pipeline
python spark/06_pipeline_runner.py

# Skip bronze (Bronze already loaded)
python spark/06_pipeline_runner.py --skip-bronze

# Single stage
python spark/06_pipeline_runner.py --stage silver
```

---

## Repository Structure

```
spark-lakehouse-statcan/
├── local-setup/
│   ├── spark/                  # PySpark pipeline scripts (01–05)
│   ├── dbt/                    # dbt project (models, tests, sources)
│   ├── delta_tables/           # Delta Lake storage (gitignored)
│   ├── spark-warehouse/        # dbt materialised tables (gitignored)
│   ├── .env                    # Environment variables (gitignored)
│   ├── requirements.txt        # All Python dependencies
│   └── README.md               # Local setup guide
└── README.md                   # This file
```

---

## What This Demonstrates

- Medallion architecture design and implementation
- Delta Lake ACID operations (merge, overwrite, time travel)
- SCD Type 2 implementation using PySpark `MERGE INTO`
- dbt model design with Spark adapter
- Pipeline orchestration with fail-fast, stage-level timing
- Environment variable management for local/CI portability
- Production-aware patterns: idempotency, schema enforcement, `.env` isolation
