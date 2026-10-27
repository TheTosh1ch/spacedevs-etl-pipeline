import logging
import time
from datetime import timedelta

import pendulum
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def _verify_silver(**context) -> None:
    import time
    import requests

    load_date = context["ds"]
    trino_url = "http://trino:8080/v1/statement"

    sql = f"""
        SELECT COUNT(*) AS cnt
        FROM iceberg.silver.launches
        WHERE load_date = DATE '{load_date}'
    """

    last_error = None
    for attempt in range(1, 6):
        try:
            r = requests.post(
                trino_url,
                data=sql.encode("utf-8"),
                headers={"X-Trino-User": "airflow"},
                timeout=30,
            )
            r.raise_for_status()
            payload = r.json()

            # Начинаем собирать данные с первой порции
            rows = payload.get("data", [])

            # Добираем данные из следующих порций
            while payload.get("nextUri"):
                payload = requests.get(payload["nextUri"], timeout=30).json()
                if "error" in payload:
                    error = payload["error"]
                    raise RuntimeError(
                        f"{error.get('errorName')}: {error.get('message')}"
                    )
                rows.extend(payload.get("data", []))

            if "error" in payload:
                error = payload["error"]
                raise RuntimeError(
                    f"{error.get('errorName', 'TRINO_ERROR')}: "
                    f"{error.get('message', 'unknown')}"
                )

            if not rows:
                raise RuntimeError("Trino returned empty data[]")

            cnt = rows[0][0]

            if cnt == 0:
                logger.info(
                    "[%s] 0 rows in silver. No launches this day. Skipping verify.",
                    load_date,
                )
                return

            logger.info("✅ Verified %d rows in silver.launches for %s", cnt, load_date)
            return

        except Exception as e:
            last_error = e
            logger.warning("Attempt %d failed: %s. Retrying in 10s...", attempt, e)
            time.sleep(10)

    raise RuntimeError(f"verify_silver failed after 5 attempts: {last_error}")


default_args = {
    "owner": "kenz0kenz",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
}

with DAG(
    dag_id="kenz0kenz_bronze_to_silver",
    default_args=default_args,
    description="bronze parquet to Iceberg silver.launches",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["thespacedevs", "silver", "iceberg", "spark"],
) as dag:

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="""
            spark-submit \
              --master local[*] \
              --conf spark.driver.memory=1g \
              /opt/airflow/spark_jobs/bronze_to_silver.py {{ ds }}
        """,
    )

    verify_silver = PythonOperator(
        task_id="verify_silver",
        python_callable=_verify_silver,
    )

    bronze_to_silver >> verify_silver
