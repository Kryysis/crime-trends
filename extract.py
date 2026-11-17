"""Socrata extraction task for the Cambridge Police Department Daily Police Log."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

os.environ.setdefault("PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW", "ignore")

from prefect import task
from sodapy import Socrata

LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_DOMAIN = "data.cambridgema.gov"
DATASET_ID = "3gki-wyrb"


@task(name="extract-cambridge-incidents", retries=3, retry_delay_seconds=[10, 10, 10])
def extract_data() -> pd.DataFrame:
    """Fetch all available incident records from the Socrata API.

    A Socrata app token is recommended to avoid stricter unauthenticated
    throttling. The public dataset can still be queried without one.
    """
    load_dotenv(PROJECT_ROOT / ".env", override=False)
    app_token = os.getenv("SOCRATA_APP_TOKEN", "").strip() or None

    if app_token is None:
        LOGGER.warning(
            "SOCRATA_APP_TOKEN is not set. Continuing without an app token; "
            "Socrata may apply stricter request throttling."
        )

    client = Socrata(DATASET_DOMAIN, app_token, timeout=60)
    results = client.get_all(DATASET_ID, order=":id ASC")
    data = pd.DataFrame.from_records(results)

    if data.empty:
        raise RuntimeError("The Socrata API returned no records. The pipeline has been stopped.")

    LOGGER.info("Extracted %s records with %s columns.", len(data), len(data.columns))
    return data
