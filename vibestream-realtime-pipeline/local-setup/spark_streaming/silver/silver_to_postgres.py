"""
Silver → PostgreSQL Loader

Reads the latest Silver Delta snapshot and loads it into PostgreSQL so dbt can run its Gold transformations against it.

In production (Databricks/AWS):
- dbt-spark or dbt-databricks reads Delta directly — no loader needed.
- This script is local-dev only workaround since dbt-postgres cannot natively read Delta Lake files.
"""

from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SilverToPostgres")

DELTA_SILVER_PATH = "/opt/delta-lake/silver/engagement_events"
POSTGRES_URL      = "jdbc:postgresql://postgres:5432/vibestream_features"
POSTGRES_PROPS    = {
    "user":   "vibestream",
    "password": "vibestream_local",
    "driver": "org.postgresql.Driver"
}

def main():
    spark = (
        SparkSession.builder
        .appName("VibeStream-Silver-To-Postgres")
        .master("spark://spark-master:7077")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    logger.info("Reading Silver Delta table...")
    silver_df = spark.read.format("delta").load(DELTA_SILVER_PATH)
    count = silver_df.count()
    logger.info(f"Silver records to load: {count}")

    logger.info("Writing to PostgreSQL silver schema...")
    (
        silver_df.write
        .option("truncate", "true")  # ← TRUNCATE instead of DROP+CREATE
        .jdbc(
            url = POSTGRES_URL,
            table = "silver.engagement_events",
            mode = "overwrite",       # Full refresh for local dev
            properties = POSTGRES_PROPS
        )
    )
    logger.info(f"Done. {count} records loaded into silver.engagement_events")
    spark.stop()

if __name__ == "__main__":
    main()