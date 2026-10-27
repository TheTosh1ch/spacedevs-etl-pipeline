"""
DAG: silver (Iceberg) -> gold (ClickHouse).
"""

import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)


default_args = {
    "owner": "kenz0kenz",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
}

with DAG(
    dag_id="kenz0kenz_silver_to_gold",
    default_args=default_args,
    description="silver Iceberg to gold ClickHouse",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["thespacedevs", "gold", "clickhouse", "spark"],
) as dag:

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="""
            spark-submit \
              --master local[*] \
              --conf spark.driver.memory=1g \
              /opt/airflow/spark_jobs/silver_to_gold.py {{ ds }}
        """,
    )
