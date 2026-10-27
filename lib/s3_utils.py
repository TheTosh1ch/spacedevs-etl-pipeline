"""
Утилиты для работы с S3/MinIO через Airflow S3Hook.
"""

import io
import logging

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)


def upload_parquet_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    conn_id: str = "minios3_conn",
) -> str:
    """
    Сериализует DataFrame в parquet и кладёт в S3.
    Возвращает полный s3:// путь.
    """
    hook = S3Hook(aws_conn_id=conn_id)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    size_bytes = buffer.tell()
    buffer.seek(0)

    hook.load_bytes(
        bytes_data=buffer.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )

    s3_path = f"s3://{bucket}/{key}"
    logger.info("Uploaded %d bytes to %s", size_bytes, s3_path)
    return s3_path


def read_parquet_from_s3(
    bucket: str,
    key: str,
    conn_id: str = "minios3_conn",
) -> pd.DataFrame:
    """
    Читает parquet из S3 в DataFrame.
    """
    hook = S3Hook(aws_conn_id=conn_id)

    obj = hook.get_key(key=key, bucket_name=bucket)
    if obj is None:
        raise FileNotFoundError(f"s3://{bucket}/{key} not found")

    body = obj.get()["Body"].read()
    return pd.read_parquet(io.BytesIO(body))
