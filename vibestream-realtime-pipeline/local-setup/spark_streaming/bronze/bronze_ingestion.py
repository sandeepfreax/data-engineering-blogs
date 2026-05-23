"""
VibeStream — Bronze Layer Ingestion (Spark Structured Streaming)

Reads raw engagement events from Kafka and writes them as-is into the Bronze Delta Lake table.

Medallion Architecture:
  Bronze → Raw events, schema enforced, no transformations
  Silver → Cleaned, deduplicated, enriched
  Gold   → Aggregated ML feature tables

Why Bronze is raw:
  Preserving the original event allows replaying/reprocessing history
  if Silver or Gold logic changes — a key requirement for ML reproducibility.

Run from spark-master container:
  docker exec vibestream-spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \--total-executor-cores 1 \
  --executor-memory 1G \
  --conf spark.driver.memory=1G \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.shuffle.partitions=6 \
  /opt/spark-apps/bronze/bronze_ingestion.py
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeIngestion")

# ── Schema — mirrors event_schema.py EngagementEvent dataclass ────────────────
ENGAGEMENT_SCHEMA = StructType([
    StructField("event_id",       StringType(),  False),
    StructField("event_type",     StringType(),  False),
    StructField("user_id",        StringType(),  False),
    StructField("content_id",     StringType(),  False),
    StructField("content_type",   StringType(),  False),
    StructField("creator_id",     StringType(),  False),
    StructField("device_type",    StringType(),  False),
    StructField("platform",       StringType(),  False),
    StructField("event_ts",       StringType(),  False),   # String; cast to timestamp in Silver
    StructField("ingestion_ts",   StringType(),  False),
    StructField("session_id",     StringType(),  False),
    StructField("watch_duration", DoubleType(),  True),    # Nullable — only WATCH events
    StructField("comment_length", IntegerType(), True),    # Nullable — only COMMENT events
])

# ── Paths — Delta Lake on local filesystem (maps to docker volume) ─────────────
DELTA_BASE          = "/opt/delta-lake"
BRONZE_TABLE_PATH   = f"{DELTA_BASE}/bronze/engagement_events"
CHECKPOINT_PATH     = f"{DELTA_BASE}/checkpoints/bronze_engagement"

# ── Kafka config ───────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP     = "kafka:29092"
KAFKA_TOPIC         = "engagement_events"


def create_spark_session() -> SparkSession:
    """
    SparkSession with Delta Lake and Kafka connectors.

    Delta Lake config:
    - spark.sql.extensions: Enables Delta-specific SQL commands (OPTIMIZE, VACUUM)
    - spark.sql.catalog.spark_catalog: Replaces default catalog with Delta's, enabling Delta tables to be queried like
        regular Spark tables.
    """
    return (
        SparkSession.builder
        .appName("VibeStream-Bronze-Ingestion")
        .master("spark://spark-master:7077")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
        # Performance tuning for local dev
        .config("spark.sql.shuffle.partitions", "6")    # Match Kafka partitions
        .config("spark.default.parallelism", "6")
        .getOrCreate()
    )


def read_from_kafka(spark: SparkSession):
    """
    Reads a streaming DataFrame from Kafka.

    Key streaming options:
    - startingOffsets='latest': Process only new events (use 'earliest' for replay)
    - maxOffsetsPerTrigger: Limits records per micro-batch (backpressure control)
    - failOnDataLoss=False: Continue if Kafka offsets are unavailable (local dev only;
      set to True in production to catch data loss issues)
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 10000)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(raw_stream):
    """
    Parses raw Kafka bytes → structured EngagementEvent columns.

    Kafka DataFrame columns: key, value (bytes), topic, partition, offset, timestamp, timestampType
    We decode value (JSON string) and apply our schema.

    Note on _kafka_* metadata columns:
    We retain partition and offset for debugging/auditing purposes in Bronze.
    These are dropped in the Silver layer.
    """
    return (
        raw_stream
        .select(
            col("value").cast("string").alias("raw_json"),
            col("partition").alias("_kafka_partition"),
            col("offset").alias("_kafka_offset"),
            col("timestamp").alias("_kafka_timestamp"),
        )
        .select(
            from_json(col("raw_json"), ENGAGEMENT_SCHEMA).alias("event"),
            col("_kafka_partition"),
            col("_kafka_offset"),
            col("_kafka_timestamp"),
        )
        .select(
            "event.*",
            "_kafka_partition",
            "_kafka_offset",
            "_kafka_timestamp",
            current_timestamp().alias("_bronze_loaded_at"),
        )
    )


def write_to_bronze_delta(parsed_stream):
    """
    Writes parsed events to the Bronze Delta Lake table.

    Trigger: processingTime='30 seconds'
    - Micro-batch every 30s. Balances latency vs. small-file problem.
    - For < 5 min latency SLA, reduce to 10s. For cost optimization, increase to 60s.

    outputMode: 'append'
    - Bronze never updates or deletes — append only.
    - This preserves full event history for replay.

    Checkpointing:
    - Spark saves Kafka offsets to the checkpoint directory after each successful batch.
    - On restart, Spark resumes from the last committed offset — exactly-once semantics.
    """
    return (
        parsed_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")
        .start(BRONZE_TABLE_PATH)
    )


def main():
    logger.info("Starting VibeStream Bronze Ingestion Job")
    spark  = create_spark_session()
    raw    = read_from_kafka(spark)
    parsed = parse_events(raw)
    query  = write_to_bronze_delta(parsed)

    logger.info("Bronze streaming query started. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
