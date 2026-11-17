"""Validation tasks for the Cambridge Police Department incident dataset."""

from __future__ import annotations

import os
from collections import Counter
from datetime import datetime

import pandas as pd

# Tasks are intentionally run as standalone Prefect tasks, not deployed flows.
os.environ.setdefault("PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW", "ignore")

from prefect import task

EXPECTED_COLUMNS = [
    "date_time",
    "id",
    "type",
    "subtype",
    "location",
    "last_updated",
    "description",
]
REQUIRED_COLUMNS = ["date_time", "id", "type", "location"]


def check_valid_schema(df: pd.DataFrame) -> None:
    """Ensure that the incoming DataFrame has the expected dataset columns."""
    if Counter(df.columns) != Counter(EXPECTED_COLUMNS):
        missing = sorted(set(EXPECTED_COLUMNS) - set(df.columns))
        unexpected = sorted(set(df.columns) - set(EXPECTED_COLUMNS))
        raise ValueError(
            "Schema does not match the expected Cambridge Police dataset. "
            f"Missing columns: {missing}; unexpected columns: {unexpected}."
        )


def check_missing_values(df: pd.DataFrame) -> None:
    """Reject records missing required source fields before ID coercion."""
    for column in REQUIRED_COLUMNS:
        if df[column].isna().any():
            count = int(df[column].isna().sum())
            raise ValueError(
                f"Required field '{column}' contains {count} missing value(s)."
            )


def check_numeric_id(df: pd.DataFrame) -> pd.DataFrame:
    """Convert IDs to numbers; malformed non-empty IDs become NaN for removal."""
    result = df.copy()
    result["id"] = pd.to_numeric(result["id"], errors="coerce")
    return result


def verify_datetime(df: pd.DataFrame) -> None:
    """Ensure every date_time value can be parsed as an ISO-style timestamp."""
    for row_index, value in df["date_time"].items():
        try:
            # datetime.fromisoformat accepts ISO 8601 values; normalise a UTC Z suffix.
            datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Invalid date_time value at row index {row_index}: {value!r}."
            ) from exc


@task(name="validate-cambridge-incidents")
def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate structure and required fields, then coerce incident IDs.

    Missing source IDs are rejected. Non-numeric, non-empty IDs are converted
    to NaN so that the transformation stage can remove those rows, matching
    the handling described in the professor's guide.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("validate_data expects a pandas DataFrame.")

    result = df.copy()
    check_valid_schema(result)
    check_missing_values(result)
    verify_datetime(result)
    result = check_numeric_id(result)

    print("Schema check passed!")
    print("Required-field check passed!")
    print("Datetime validation passed!")
    print("ID conversion completed; non-numeric IDs are marked for removal.")
    return result
