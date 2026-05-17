# data-engineering-blogs

> **Hands-on data engineering — built in public.**

This repository is a living record of real, end-to-end data engineering projects built from scratch. Every project here reflects practical experience with the tools, patterns, and decisions that define modern data engineering — not tutorials, not toy datasets, but production-grade thinking applied to real data.

The goal is simple: build things that matter, document what was learned, and share it openly.

---

## Why This Exists

Data engineering is learned by doing. Blog posts explain concepts; this repository proves them. Each project tackles a real problem — data ingestion, transformation, modelling, orchestration — using the same tools and architectural patterns used in production environments at scale.

This is my engineering notebook, version-controlled and open source.

---

## Projects

| Project | Description | Stack |
|---------|-------------|-------|
| [`spark-lakehouse-statcan`](./spark-lakehouse-statcan/) | End-to-end lakehouse pipeline on Canadian Labour Force Survey data — Bronze → Silver → Gold → dbt | PySpark · Delta Lake · dbt · Python |

> More projects coming. Each will follow the same standard: a real dataset, a real problem, a fully documented solution.

---

## What You'll Find Here

- **Full pipeline implementations** — not snippets, but complete, runnable solutions
- **Architecture decision records** — why each design choice was made
- **Local-first development** — every project runs on a laptop before touching the cloud
- **Production-aware patterns** — SCD2, medallion architecture, data quality, idempotent runs

---

## Tech Philosophy

- Local setup first, cloud-ready by design
- Reproducible environments via `requirements.txt`
- Pipelines that fail loudly and recover cleanly
- Documentation written for engineers, not for show

---

## Connect

- GitHub: [@sandeepfreax](https://github.com/sandeepfreax)
- LinkedIn: [Sandeep Kumar](https://www.linkedin.com/in/sandeep-freax/)
