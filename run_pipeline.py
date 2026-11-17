"""Run the ETL tasks and write the latest output, summary and log files."""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_ROOT / "output"
LOG_DIR = PROJECT_ROOT / "logs"
SUMMARY_PATH = OUTPUT_DIR / "latest_summary.json"
CSV_PATH = OUTPUT_DIR / "transformed_data.csv"
LOCK_PATH = OUTPUT_DIR / "pipeline.lock"
DATASET_ID = "3gki-wyrb"

# Standalone Prefect tasks are used; no Prefect Cloud or scheduled deployment is required.
os.environ.setdefault("PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW", "ignore")

from extract import extract_data  # noqa: E402
from transform import transform_data  # noqa: E402
from validate import validate_data  # noqa: E402


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def configure_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("crime_trends")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler = logging.FileHandler(LOG_DIR / "pipeline.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    temporary_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, allow_nan=False),
        encoding="utf-8",
    )
    temporary_path.replace(path)


def make_initial_summary(run_id: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "run_id": run_id,
        "status": "running",
        "dataset_id": DATASET_ID,
        "source_name": "Cambridge Police Department Daily Police Log",
        "started_at": utc_now(),
        "completed_at": None,
        "duration_seconds": None,
        "current_stage": "initialising",
        "stages": {
            "extraction": {"status": "pending", "started_at": None, "completed_at": None},
            "validation": {"status": "pending", "started_at": None, "completed_at": None},
            "transformation": {"status": "pending", "started_at": None, "completed_at": None},
        },
        "metrics": {
            "records_extracted": None,
            "raw_duplicate_rows": None,
            "duplicate_ids_detected": None,
            "invalid_ids_detected": None,
            "records_transformed": None,
            "missing_subtype": None,
            "missing_description": None,
        },
        "output_file": None,
        "error": None,
    }


def set_stage(summary: dict[str, Any], stage: str, status: str) -> None:
    stage_info = summary["stages"][stage]
    stage_info["status"] = status
    if status == "running":
        stage_info["started_at"] = utc_now()
    if status in {"passed", "failed"}:
        stage_info["completed_at"] = utc_now()


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger = configure_logging()
    load_dotenv(PROJECT_ROOT / ".env", override=False)

    try:
        # Exclusive creation prevents two executions overwriting the same latest output.
        with LOCK_PATH.open("x", encoding="utf-8") as lock_file:
            lock_file.write(f"pid={os.getpid()}\nstarted_at={utc_now()}\n")
    except FileExistsError:
        logger.error(
            "A pipeline lock already exists (%s). Confirm no run is active before removing it.",
            LOCK_PATH,
        )
        return 2

    run_id = str(uuid.uuid4())
    summary = make_initial_summary(run_id)
    started_clock = time.monotonic()
    write_json_atomic(SUMMARY_PATH, summary)
    logger.info("Starting Crime Trends ETL run %s", run_id)

    try:
        summary["current_stage"] = "extraction"
        set_stage(summary, "extraction", "running")
        write_json_atomic(SUMMARY_PATH, summary)
        logger.info("Extracting records from Socrata dataset %s", DATASET_ID)
        raw_data = extract_data()
        summary["metrics"].update(
            {
                "records_extracted": int(len(raw_data)),
                "raw_duplicate_rows": int(raw_data.duplicated().sum()),
                "missing_subtype": int(raw_data["subtype"].isna().sum()) if "subtype" in raw_data else None,
                "missing_description": int(raw_data["description"].isna().sum()) if "description" in raw_data else None,
            }
        )
        if raw_data.empty:
            raise RuntimeError("The Socrata API returned no records; stopping before validation.")

        set_stage(summary, "extraction", "passed")
        logger.info("Extraction completed: %s records.", len(raw_data))
        write_json_atomic(SUMMARY_PATH, summary)

        summary["current_stage"] = "validation"
        set_stage(summary, "validation", "running")
        write_json_atomic(SUMMARY_PATH, summary)
        logger.info("Validating source records")
        validated_data = validate_data(raw_data)
        valid_ids = validated_data["id"].dropna()
        summary["metrics"].update(
            {
                "duplicate_ids_detected": int(valid_ids.duplicated().sum()),
                "invalid_ids_detected": int(validated_data["id"].isna().sum()),
            }
        )
        set_stage(summary, "validation", "passed")
        logger.info("Validation completed successfully.")
        write_json_atomic(SUMMARY_PATH, summary)

        summary["current_stage"] = "transformation"
        set_stage(summary, "transformation", "running")
        write_json_atomic(SUMMARY_PATH, summary)
        logger.info("Transforming validated records")
        transformed_data = transform_data(validated_data)
        if transformed_data.empty:
            raise RuntimeError("No records remain after transformation; no output was published.")

        # Write atomically so the web interface never reads a partially written CSV.
        temporary_csv = CSV_PATH.with_suffix(".csv.tmp")
        transformed_data.to_csv(temporary_csv, index=False, encoding="utf-8")
        temporary_csv.replace(CSV_PATH)

        summary["metrics"]["records_transformed"] = int(len(transformed_data))
        summary["output_file"] = "output/transformed_data.csv"
        set_stage(summary, "transformation", "passed")
        summary["status"] = "success"
        summary["current_stage"] = "completed"
        summary["completed_at"] = utc_now()
        summary["duration_seconds"] = round(time.monotonic() - started_clock, 2)
        write_json_atomic(SUMMARY_PATH, summary)

        logger.info("Transformation completed: %s records remain.", len(transformed_data))
        logger.info("Output saved to %s", CSV_PATH)
        logger.info("ETL run completed successfully in %.2f seconds.", summary["duration_seconds"])
        return 0

    except Exception as exc:
        logger.exception("ETL run failed.")
        current_stage = summary.get("current_stage")
        if current_stage in summary["stages"] and summary["stages"][current_stage]["status"] == "running":
            set_stage(summary, current_stage, "failed")
        summary["status"] = "failed"
        summary["completed_at"] = utc_now()
        summary["duration_seconds"] = round(time.monotonic() - started_clock, 2)
        summary["error"] = {"type": type(exc).__name__, "message": str(exc)[:600]}
        write_json_atomic(SUMMARY_PATH, summary)
        return 1

    finally:
        try:
            LOCK_PATH.unlink(missing_ok=True)
        except OSError:
            logger.exception("Could not remove pipeline lock file %s", LOCK_PATH)


if __name__ == "__main__":
    raise SystemExit(main())
