"""
Клиент для thespacedevs API.
Тянет данные о запусках ракет за конкретную дату.
"""
import logging
import time
from datetime import datetime, timezone

import pandas as pd
import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://ll.thespacedevs.com/2.3.0/launches/"


def fetch_launches(load_date: str, limit: int = 100, max_retries: int = 3) -> pd.DataFrame:
    """
    Загружает запуски за указанную дату.

    :param load_date: дата в формате YYYY-MM-DD
    :param limit: сколько записей максимум тянуть из API
    :param max_retries: сколько раз повторять при ошибке
    :return: DataFrame с данными или пустой DataFrame
    """
    start_of_day = f"{load_date}T00:00:00Z"
    end_of_day = f"{load_date}T23:59:59Z"

    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                BASE_URL,
                params={
                    "net__gte": start_of_day,
                    "net__lte": end_of_day,
                    "ordering": "-window_start",
                    "limit": limit,
                },
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])

            if not results:
                logger.warning("No launches found for %s", load_date)
                return pd.DataFrame()

            df = pd.json_normalize(results)
            df["ingested_at"] = datetime.now(timezone.utc).isoformat()
            df["load_date"] = load_date

            logger.info("Fetched %d rows for %s", len(df), load_date)
            return df

        except requests.exceptions.HTTPError as e:
            last_error = e
            status = response.status_code

            if status == 429:
                wait = 5 * attempt
                logger.warning("Rate limited (429), sleeping %ds", wait)
                time.sleep(wait)
            elif 500 <= status < 600:
                logger.warning("Server error %d, retrying", status)
                time.sleep(5)
            else:
                logger.error("HTTP error %d: %s", status, e)
                raise

        except requests.exceptions.RequestException as e:
            last_error = e
            logger.warning("Attempt %d failed: %s", attempt, e)
            time.sleep(5)

    raise RuntimeError(f"All {max_retries} retries failed: {last_error}")