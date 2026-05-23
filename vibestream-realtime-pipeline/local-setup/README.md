# Building a Real-Time Social Media Analytics Pipeline

> Kafka 4.0 (KRaft) → Spark Structured Streaming → Delta Lake → dbt → ML Feature Store

Companion code for the Medium article: *"How VibeStream (Fictional) Cut ML Feature Latency from 24 Hours to 4 Minutes"*

## Architecture

```
[VibeStream (Fictionals) App]
      │
      ▼ (5M events/day)
[Kafka 4.0 KRaft] ──── topic: engagement_events (6 partitions)
      │
      ▼ (Spark Structured Streaming)
[Bronze Delta Table] ── raw events, append-only
      │
      ▼ (Spark Structured Streaming + MERGE)
[Silver Delta Table] ── cleaned, deduplicated, watermarked
      │
      ▼ (dbt incremental models)
[Gold PostgreSQL] ────── user_engagement_features, content_trending_features
      │
      ▼
[ML Recommendation Model] ── consumes feature tables (< 5 min latency)
```

## Prerequisites

- Docker Desktop (8GB+ RAM allocated)
- Python 3.11
- `pip install -r requirements.txt` 

## Quick Start

```bash
# 1. Start the full stack
cd docker
docker compose up -d

# 2. Wait for Kafka to be healthy (~30s), then verify topics
docker exec vibestream-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 --list

# 3. Run the event producer (from kafka_producer/)
python vibestream_producer.py --events-per-sec 50 --duration 600

# 4. Submit Spark streaming jobs
docker exec vibestream-spark-master spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-apps/bronze/bronze_ingestion.py

# 5. Access UIs
#   Spark Master:  http://localhost:8080
#   Spark App UI:  http://localhost:4040
#   Jupyter Lab:   http://localhost:8888 (token: vibestream)
```

## Services

| Service | Port | URL |
|---------|------|-----|
| Kafka (KRaft) | 9092 | `localhost:9092` |
| Spark Master UI | 8080 | http://localhost:8080 |
| Spark App UI | 4040 | http://localhost:4040 |
| Jupyter Lab | 8888 | http://localhost:8888 |
| PostgreSQL | 5432 | `localhost:5432` |

## Folder Structure

```
01-vibestream-realtime-pipeline/
├── docker/
│   ├── docker-compose.yml          ← Full stack: Kafka + Spark + Postgres + Jupyter
│   └── postgres-init/              ← Schema initialization SQL
├── kafka_producer/
│   ├── event_schema.py             ← Canonical event schema + dataclasses
│   └── vibestream_producer.py      ← Realistic event simulator
├── spark_streaming/
│   ├── bronze/bronze_ingestion.py  ← Kafka → Bronze Delta Lake
│   └── silver/silver_transform.py  ← Bronze → Silver (clean + deduplicate)
├── dbt/
│   └── models/gold/                ← Silver → Gold ML Feature tables
└── notebooks/                      ← Exploration notebooks
```
