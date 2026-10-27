"""
Spark-job: читает silver.launches из Iceberg, считает агрегаты за конкретную дату,
пишет в ClickHouse (база gold) инкрементально.
"""

import logging
import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CATALOG = "iceberg"
SILVER_TABLE = f"{CATALOG}.silver.launches"

MINIO_USER = "minioadmin"
MINIO_PASSWORD = "minioadmin"
MINIO_ENDPOINT = "http://minio:9000"
ICEBERG_REST_URI = "http://iceberg-rest:8181"

CLICKHOUSE_HOST = "host.docker.internal"
CLICKHOUSE_PORT = "8125"
CLICKHOUSE_DB = "gold"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = ""

JDBC_URL = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
JDBC_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("silver_to_gold")
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
        .getOrCreate()
    )


def write_clickhouse(df, table: str) -> None:
    """Просто пишет DataFrame в ClickHouse. Дедупликация на стороне CH."""
    logger.info("Writing %d rows to gold.%s", df.count(), table)
    (
        df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("driver", JDBC_DRIVER)
        .mode("append")
        .save()
    )
    logger.info("Written gold.%s", table)


def main(load_date: str) -> None:
    logger.info("Starting INCREMENTAL silver_to_gold for %s", load_date)
    spark = build_spark()

    # 1. Читаем ТОЛЬКО нужную партицию из Iceberg
    logger.info("Reading partition load_date=%s from %s", load_date, SILVER_TABLE)
    silver_df = spark.read.table(SILVER_TABLE).filter(F.col("load_date") == load_date)

    row_count = silver_df.count()
    logger.info("Silver rows for %s: %d", load_date, row_count)

    if row_count == 0:
        logger.warning("No data for %s, finishing.", load_date)
        spark.stop()
        return

    # Добавляем колонку-версию для ReplacingMergeTree
    silver_df = silver_df.withColumn("processed_at", F.current_timestamp())

    # 2. Считаем агрегаты
    by_country = (
        silver_df.withColumn(
            "country", F.split(F.col("pad_location_name"), ",").getItem(F.lit(1))
        )
        .withColumn("country", F.trim(F.col("country")))
        .groupBy(F.col("load_date").alias("launch_date"), "country")  # <--- ИСПРАВЛЕНО
        .agg(
            F.count("*").alias("launches_count"),
            F.max("processed_at").alias("processed_at"),
        )
        .filter(F.col("country").isNotNull())
    )

    rocket_stats = (
        silver_df.groupBy(
            F.col("load_date").alias("launch_date"), "rocket_name"
        )  # <--- ИСПРАВЛЕНО
        .agg(
            F.count("*").alias("total_launches"),
            F.sum(F.when(F.col("status_abbrev") == "Success", 1).otherwise(0)).alias(
                "successful"
            ),
            F.sum(F.when(F.col("status_abbrev") == "Failure", 1).otherwise(0)).alias(
                "failed"
            ),
            F.max("processed_at").alias("processed_at"),
        )
        .withColumn(
            "success_rate",
            F.round(F.col("successful") / F.col("total_launches") * 100, 2),
        )
        .filter(F.col("rocket_name").isNotNull())
    )

    agencies = (
        silver_df.groupBy(
            F.col("load_date").alias("launch_date"), "provider_name"
        )  # <--- ИСПРАВЛЕНО
        .agg(
            F.count("*").alias("launches_count"),
            F.max("processed_at").alias("processed_at"),
        )
        .filter(F.col("provider_name").isNotNull())
    )

    # 3. Просто пишем в ClickHouse (append)
    write_clickhouse(by_country, "launches_by_country_day")
    write_clickhouse(rocket_stats, "rocket_success_rate")
    write_clickhouse(agencies, "agency_top_10")

    spark.stop()
    logger.info("Done.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: silver_to_gold.py <load_date YYYY-MM-DD>")
    main(sys.argv[1])
