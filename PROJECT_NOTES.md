# Implementation notes

- The source notebook's intended invalid-ID handling was retained: truly missing required IDs fail validation, while non-empty IDs that cannot be converted to numbers become `NaN` and are removed during transformation. In the source notebook, IDs were converted before missing values were checked, which caused malformed non-empty IDs to fail validation before transformation could remove them. The order was corrected.
- The transformation retains rows with `NaN` IDs until the invalid-row removal step so the invalid-ID count can be measured accurately. Valid duplicate incident IDs are deduplicated, keeping the first occurrence.
- The ETL runner invokes the three Prefect tasks but intentionally does not define a Prefect `@flow`, deploy a flow, or schedule recurring executions.
- The dashboard's displayed data and run information are read from `output/latest_summary.json` and `output/transformed_data.csv`. The dashboard does not display the `.env` contents or PostgreSQL credentials.
- `check_postgres.py` is separate from the ETL runner. PostgreSQL availability is an environment connectivity check, not an ETL load step. Transformed records are currently written to CSV and are not inserted into PostgreSQL.
