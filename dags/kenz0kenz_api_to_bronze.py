"""
DAG: API thespacedevs → S3 (bronze layer).

Схема:
    extract_to_bronze  →  quality_check

Поведение при отсутствии данных за дату:
    extract_to_bronze завершается успешно, xcom.total_rows = 0.
    quality_check видит, что данных нет, и переходит в статус skipped.
    DAG завершается успешно (Success). Отсутствие запусков —
    нормальная бизнес-ситуация, а не ошибка.
"""

import logging
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/lib")

from api_client import fetch_launches  # noqa: E402
from s3_utils import read_parquet_from_s3, upload_parquet_to_s3  # noqa: E402

logger = logging.getLogger(__name__)

BRONZE_BUCKET = "lakehouse"
BRONZE_PREFIX = "bronze/launches"
S3_CONN_ID = "minios3_conn"


def _extract_to_bronze(**context) -> None:
    """
    Тянет данные из API и кладёт в S3 как parquet.

    Если данных нет — задача всё равно Success, но в xcom пишется
    total_rows=0, чтобы quality_check знал, что проверять нечего.
    """
    load_date = context["ds"]
    ti = context["ti"]

    logger.info("Starting extract for %s", load_date)

    df = fetch_launches(load_date)

    if df.empty:
        logger.warning("No launches for %s — nothing to upload", load_date)
        ti.xcom_push(key="load_date", value=load_date)
        ti.xcom_push(key="total_rows", value=0)
        ti.xcom_push(key="s3_key", value="")
        return

    key = f"{BRONZE_PREFIX}/dt={load_date}/data.parquet"
    s3_path = upload_parquet_to_s3(df, BRONZE_BUCKET, key, conn_id=S3_CONN_ID)

    ti.xcom_push(key="load_date", value=load_date)
    ti.xcom_push(key="total_rows", value=len(df))
    ti.xcom_push(key="s3_key", value=key)

    logger.info("Extracted %d rows to %s", len(df), s3_path)


def _quality_check(**context) -> None:
    """
    Проверяет данные в S3.

    Если данных за дату нет — задача уходит в skipped.
    Это не ошибка, просто нечего проверять.
    """
    ti = context["ti"]

    load_date = ti.xcom_pull(key="load_date", task_ids="extract_to_bronze")
    total_rows = ti.xcom_pull(key="total_rows", task_ids="extract_to_bronze")
    s3_key = ti.xcom_pull(key="s3_key", task_ids="extract_to_bronze")

    # Нет данных — не ошибка, а нормальный «пустой» день.
    # Переводим таск в skipped, чтобы не поднимать ложных алертов.
    if total_rows == 0:
        logger.info(
            "[%s] No launches for this date — skipping quality check", load_date
        )
        raise AirflowSkipException(f"No data for {load_date}")

    # Данные есть — проверяем по существу
    if total_rows > 100:
        raise ValueError(f"[{load_date}] Too many rows: {total_rows} (expected <= 100)")

    df = read_parquet_from_s3(BRONZE_BUCKET, s3_key, conn_id=S3_CONN_ID)

    if len(df) != total_rows:
        raise ValueError(
            f"[{load_date}] Row count mismatch: xcom={total_rows}, s3={len(df)}"
        )

    required_cols = {"id", "name", "net", "load_date"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"[{load_date}] Missing columns: {missing}")

    if df["id"].isnull().any():
        raise ValueError(f"[{load_date}] Found null 'id' — data corrupted")

    if df["id"].duplicated().any():
        raise ValueError(f"[{load_date}] Duplicated 'id' found")

    logger.info(
        "✅ Quality check passed: %d rows, %d columns", len(df), len(df.columns)
    )


default_args = {
    "owner": "kenz0kenz",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="kenz0kenz_api_to_bronze",
    default_args=default_args,
    description="thespacedevs API → S3 bronze (parquet)",
    schedule="@daily",
    start_date=pendulum.today("UTC").subtract(days=7),
    catchup=False,
    max_active_runs=1,
    tags=["thespacedevs", "bronze", "api"],
) as dag:

    extract_to_bronze = PythonOperator(
        task_id="extract_to_bronze",
        python_callable=_extract_to_bronze,
    )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=_quality_check,
    )

    extract_to_bronze >> quality_check
