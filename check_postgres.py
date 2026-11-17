"""Verify that the configured local PostgreSQL service is reachable."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env", override=False)


def main() -> int:
    required = ["POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        print("Missing database configuration in .env: " + ", ".join(missing))
        return 2

    try:
        connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
            port=int(os.getenv("POSTGRES_PORT", "5433")),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            connect_timeout=5,
        )
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT current_database(), version();")
                database_name, version = cursor.fetchone()
        finally:
            connection.close()
        print(f"Connected to PostgreSQL database: {database_name}")
        print(f"Server version: {version.split(',')[0]}")
        return 0
    except (psycopg2.Error, ValueError) as exc:
        print(f"PostgreSQL connection check failed: {type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
