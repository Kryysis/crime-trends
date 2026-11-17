"""Transformation tasks for the Cambridge Police Department incident dataset."""

from __future__ import annotations

import os

import pandas as pd

os.environ.setdefault("PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW", "ignore")

from prefect import task


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Keep the first occurrence of each valid incident ID.

    Rows with NaN IDs are retained here so they can all be counted and removed
    explicitly by remove_invalid_rows in the next transformation step.
    """
    keep_mask = df["id"].isna() | ~df["id"].duplicated(keep="first")
    return df.loc[keep_mask].copy()


def remove_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows whose incident IDs could not be converted to numbers."""
    return df.dropna(subset=["id"]).copy()


def split_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Convert date_time and derive year, month, day, hour, minute and second."""
    result = df.copy()
    result["date_time"] = pd.to_datetime(result["date_time"], errors="raise")

    result["year"] = result["date_time"].dt.year
    result["month"] = result["date_time"].dt.month
    result["day"] = result["date_time"].dt.day
    result["hour"] = result["date_time"].dt.hour
    result["minute"] = result["date_time"].dt.minute
    result["second"] = result["date_time"].dt.second
    return result


@task(name="transform-cambridge-incidents")
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate incidents, remove invalid IDs and derive time attributes."""
    if not isinstance(df, pd.DataFrame):
        raise TypeError("transform_data expects a pandas DataFrame.")

    result = df.copy()
    result = remove_duplicates(result)
    print("Duplicate incidents removed!")

    result = remove_invalid_rows(result)
    print("Invalid IDs removed!")

    result = split_datetime(result)
    print("Datetime transformation completed!")
    return result
