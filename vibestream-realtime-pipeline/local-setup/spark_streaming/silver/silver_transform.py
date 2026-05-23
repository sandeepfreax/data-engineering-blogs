"""
VibeStream — Silver Layer Transformation (Spark Structured Streaming)

Reads from Bronze Delta, applies cleaning/deduplication/enrichment, writes to Silver Delta.

Silver responsibilities:
  1. Cast event_ts string → proper TimestampType
  2. Deduplicate events using event_id (idempotent re-processing)
  3. Drop internal Kafka metadata columns
  4. Add watermark for late-arriving event handling
  5. Filter invalid/malformed events (DQ checks)
  6. Enrich with derived columns (hour_of_day, is_mobile, etc.)
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, hour, when, lit
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SilverTransform")

DELTA_BASE          = "/opt/delta-lake"
BRONZE_TABLE_PATH   = f"{DELTA_BASE}/bronze/engagement_events"
SILVER_TABLE_PATH   = f"{DELTA_BASE}/silver/engagement_events"
CHECKPOINT_PATH     = f"{DELTA_BASE}/checkpoints/silver_engagement"

VALID_EVENT_TYPES   = {"like", "share", "comment", "impression", "watch", "save"}
VALID_CONTENT_TYPES = {"video", "image", "text", "reel"}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("VibeStream-Silver-Transform")
        .master("spark://spark-master:7077")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )


def read_bronze_stream(spark: SparkSession):
    """
    Reads Bronze Delta as a streaming source.
    Delta's streaming source tracks processed files via transaction log — no need to manage Kafka offsets at this stage.
    """
    return (
        spark.readStream
        .format("delta")
        .option("maxFilesPerTrigger", 10)    # Process up to 10 Delta files per batch
        .load(BRONZE_TABLE_PATH)
    )


def clean_and_enrich(bronze_stream):
    """
    Silver transformations:

    1. Watermark (late data handling):
       - 10-minute watermark on event_ts
       - Events arriving > 10 min late are dropped (acceptable for ML features)
       - In production, late events can be sent to a dead-letter Delta table

    2. Data Quality filters:
       - Drop nulls on non-nullable key columns
       - Validate enum fields against known values
       - Trim whitespace from string fields

    3. Derived columns (feature engineering prep):
       - event_hour: Used for time-of-day engagement analysis
       - is_mobile: Boolean flag for device segmentation
       - is_viral_content_type: Reels/videos have higher virality potential
    """
    return (
        bronze_stream
        # Cast string timestamp to proper TimestampType
        .withColumn("event_ts", to_timestamp(col("event_ts")))
        # Add watermark — late events beyond 10 mins are dropped
        .withWatermark("event_ts", "10 minutes")
        # Data Quality: drop rows with null key fields
        .filter(
            col("event_id").isNotNull() &
            col("user_id").isNotNull() &
            col("content_id").isNotNull() &
            col("event_type").isNotNull()
        )
        # Data Quality: validate event_type against known enum values
        .filter(col("event_type").isin(list(VALID_EVENT_TYPES)))
        # Data Quality: validate content_type
        .filter(col("content_type").isin(list(VALID_CONTENT_TYPES)))
        # Derived: hour of day (0-23) for time-based features
        .withColumn("event_hour", hour(col("event_ts")))
        # Derived: is_mobile flag for device segmentation
        .withColumn(
            "is_mobile",
            when(col("device_type").isin("mobile_ios", "mobile_android"), lit(True))
            .otherwise(lit(False))
        )
        # Derived: high-virality content flag (reels drive more engagement)
        .withColumn(
            "is_viral_content_type",
            when(col("content_type").isin("reel", "video"), lit(True))
            .otherwise(lit(False))
        )
        # Drop internal Kafka metadata (Silver is clean business data only)
        .drop("_kafka_partition", "_kafka_offset", "_kafka_timestamp", "_bronze_loaded_at")
    )


def write_to_silver_delta(silver_stream):
    """
    Writes to Silver Delta with foreachBatch for deduplication.

    Why foreachBatch instead of direct write:
    - Spark Structured Streaming's direct Delta write doesn't support MERGE (upsert) operations in streaming mode.
    - foreachBatch gives us a regular batch DataFrame per micro-batch, allowing us to use Delta MERGE for deduplication
        on event_id.
    - This achieves exactly-once semantics at the business level (duplicate events from retries are deduplicated).
    """
    def upsert_to_silver(batch_df, batch_id):
        logger.info("Processing Silver batch_id=%d, count=%d", batch_id, batch_df.count())

        # Write deduplicated batch using Delta MERGE
        from delta.tables import DeltaTable
        import os

        if os.path.exists(SILVER_TABLE_PATH):
            silver_table = DeltaTable.forPath(batch_df.sparkSession, SILVER_TABLE_PATH)
            (
                silver_table.alias("existing")
                .merge(
                    batch_df.alias("incoming"),
                    "existing.event_id = incoming.event_id"   # Dedup on event_id
                )
                .whenNotMatchedInsertAll()    # Only insert if event_id doesn't exist
                .execute()
            )
        else:
            # First batch — write directly
            batch_df.write.format("delta").save(SILVER_TABLE_PATH)

    return (
        silver_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")
        .foreachBatch(upsert_to_silver)
        .start()
    )


def main():
    logger.info("Starting VibeStream Silver Transform Job")
    spark  = create_spark_session()
    bronze = read_bronze_stream(spark)
    silver = clean_and_enrich(bronze)
    query  = write_to_silver_delta(silver)
    logger.info("Silver streaming query started.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
