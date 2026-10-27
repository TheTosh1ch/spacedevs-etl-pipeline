"""
Spark-job: читает bronze parquet из S3, чистит и пишет в Iceberg silver.launches.
"""

import logging
import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CATALOG = "iceberg"
SILVER_NAMESPACE = "silver"
SILVER_TABLE = f"{CATALOG}.{SILVER_NAMESPACE}.launches"
BRONZE_PATH = "s3a://lakehouse/bronze/launches"

MINIO_USER = "minioadmin"
MINIO_PASSWORD = "minioadmin"
MINIO_ENDPOINT = "http://minio:9000"
ICEBERG_REST_URI = "http://iceberg-rest:8181"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("bronze_to_silver")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", ICEBERG_REST_URI)
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", "s3://lakehouse")
        .config(
            f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", MINIO_ENDPOINT)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.s3.access-key-id", MINIO_USER)
        .config(f"spark.sql.catalog.{CATALOG}.s3.secret-access-key", MINIO_PASSWORD)
        .config(f"spark.sql.catalog.{CATALOG}.s3.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def ensure_namespace(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{SILVER_NAMESPACE}")
    logger.info("Namespace %s.%s ensured", CATALOG, SILVER_NAMESPACE)


def ensure_table(spark: SparkSession) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            id STRING,
            name STRING,
            net TIMESTAMP,
            status_abbrev STRING,
            provider_name STRING,
            rocket_name STRING,
            pad_name STRING,
            pad_location_name STRING,
            mission_name STRING,
            ingested_at TIMESTAMP,
            load_date DATE
        )
        USING iceberg
        PARTITIONED BY (load_date)
        TBLPROPERTIES ('format-version' = '2')
    """)
    logger.info("Table %s ensured", SILVER_TABLE)


def transform(bronze_df):
    return bronze_df.select(
        F.col("id").cast("string").alias("id"),
        F.col("name").cast("string").alias("name"),
        F.to_timestamp("net").alias("net"),
        F.col("`status.abbrev`").cast("string").alias("status_abbrev"),
        F.col("`launch_service_provider.name`").cast("string").alias("provider_name"),
        F.col("`rocket.configuration.name`").cast("string").alias("rocket_name"),
        F.col("`pad.name`").cast("string").alias("pad_name"),
        F.col("`pad.location.name`").cast("string").alias("pad_location_name"),
        F.col("`mission.name`").cast("string").alias("mission_name"),
        F.to_timestamp("ingested_at").alias("ingested_at"),
        F.to_date("load_date").alias("load_date"),
    )


def main(load_date: str) -> None:
    logger.info("Starting bronze_to_silver for %s", load_date)
    spark = build_spark()

    ensure_namespace(spark)
    ensure_table(spark)

    bronze_path = f"{BRONZE_PATH}/dt={load_date}/data.parquet"
    logger.info("Reading %s", bronze_path)

    try:
        bronze_df = spark.read.parquet(bronze_path)
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e):
            logger.warning(
                "⚠️ No data found for %s (PATH_NOT_FOUND). Skipping.", load_date
            )
            spark.stop()
            return
        else:
            raise e

    logger.info("Bronze rows: %d", bronze_df.count())

    silver_df = transform(bronze_df)

    # === ДЕДУПЛИКАЦИЯ ===
    # Оставляем только уникальные записи по id.
    # Это защитит от дублей, если API отдал одну и ту же миссию дважды.
    silver_df = silver_df.dropDuplicates(["id"])

    # Идемпотентность: удаляем партицию за дату перед вставкой
    spark.sql(f"DELETE FROM {SILVER_TABLE} WHERE load_date = DATE '{load_date}'")
    silver_df.writeTo(SILVER_TABLE).append()

    logger.info("Silver rows for %s: %d", load_date, silver_df.count())
    spark.stop()
    logger.info("Done.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: bronze_to_silver.py <load_date YYYY-MM-DD>")
    main(sys.argv[1])
