import subprocess, time, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Orchestrator")

INTERVAL_SECS = 300   # 5 minutes

def run_silver_loader():
    logger.info("Running Silver → Postgres loader...")
    subprocess.run([
        "docker", "exec", "vibestream-spark-master",
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--total-executor-cores", "1",
        "--executor-memory", "1G",
        "--conf", "spark.driver.memory=1G",
        "--packages", "io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.3",
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf", "spark.sql.shuffle.partitions=6",
        "/opt/spark-apps/silver/silver_to_postgres.py"
    ], check=True)

def run_dbt_gold():
    logger.info("Running dbt Gold models...")
    subprocess.run([
        "dbt", "run",
        "--project-dir",
        "dbt"
    ], check=True)

def main():
    logger.info("Pipeline Orchestrator started — refresh every %ds", INTERVAL_SECS)
    while True:
        try:
            run_silver_loader()
            run_dbt_gold()
            logger.info("Gold features refreshed. Next run in %ds", INTERVAL_SECS)
        except Exception as e:
            logger.error("Orchestration cycle failed: %s", e)
        time.sleep(INTERVAL_SECS)

if __name__ == "__main__":
    main()