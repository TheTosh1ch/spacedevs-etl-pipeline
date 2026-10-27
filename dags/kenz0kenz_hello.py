from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def hello(**context):
    print(f"Hello from Airflow! Date: {context['ds']}")
    print(f"MinIO user is: {context['ti']}")


default_args = {
    "owner": "kenz0kenz",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="kenz0kenz_hello",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Smoke test DAG",
    tags=["smoke-test"],
) as dag:
    PythonOperator(
        task_id="hello",
        python_callable=hello,
    )
