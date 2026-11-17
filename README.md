# Crime Trends — Police Log ETL and Analytics Dashboard

A modular data-engineering project that extracts, validates, and transforms records from the Cambridge Police Department Daily Police Log, then presents the latest pipeline run and descriptive trends in a Streamlit dashboard.

The project uses Python, Pandas, Prefect tasks, the Socrata Open Data API, and Streamlit. PostgreSQL is provided as an optional Docker service for a connectivity check; **the current ETL pipeline writes its transformed data to CSV and does not load records into PostgreSQL**.

> **Data interpretation:** The source is a daily police log containing police-log entries and calls for service. An entry should not automatically be interpreted as a confirmed crime or proven offence. Dashboard labels and conclusions should retain that distinction.

## What the project does

1. **Extract** records from the Cambridge Open Data Socrata API.
2. **Validate** the expected columns, required values, IDs, and date-time values.
3. **Transform** the data by removing repeated IDs and invalid IDs, parsing the incident date-time, and deriving year, month, day, hour, minute, and second fields.
4. **Save** the transformed records as a CSV and write a JSON summary of the pipeline run.
5. **Display** pipeline status, stage information, record metrics, a data preview, descriptive charts, and a CSV download in Streamlit.

The project separates extraction, validation, and transformation into modules. Those functions are defined as Prefect tasks; `run_pipeline.py` coordinates their execution. There is no deployed Prefect flow or recurring Prefect schedule in this version.

## Current scope and limitations

### Implemented

- Extraction from the Cambridge Open Data Socrata dataset.
- Explicit ordering by Socrata's `:id` field during extraction (`:id ASC`) to make paginated retrieval more consistent between runs.
- Schema and data validation.
- Duplicate-ID and invalid-ID handling.
- Date-time parsing and derived date/time columns.
- Prefect task decorators and extraction retries.
- CSV output and a machine-readable JSON run summary.
- A Streamlit dashboard with run status, stage status, metrics, data preview, descriptive analytics, and CSV download.
- A dashboard action that lets a visitor request a new pipeline run on the machine hosting Streamlit.
- A lock mechanism to prevent overlapping pipeline executions.
- An optional PostgreSQL 17 Docker Compose service and a separate connectivity-check script.

### Not implemented

- Loading transformed records into PostgreSQL tables.
- Metabase or another separate BI platform.
- A deployed Prefect flow, Prefect Cloud, or automatic recurring extraction schedule.
- User authentication or built-in HTTPS for the Streamlit dashboard.

The ordering of extraction improves consistency when the API returns records over multiple pages, but it is not a frozen snapshot of a changing public dataset. The source data can change between runs, so row counts and date coverage are not fixed targets.

## Dashboard behaviour

The Streamlit dashboard presents the latest available run summary and transformed CSV. Its analytics include monthly volume, common types and subtypes, incident hours, frequently occurring locations, and a weekday-by-hour view.

The preview is sorted by incident date-time in descending order so that the **15 most recent incident records** appear first. The date-coverage text is calculated from the incident dates present in the loaded dataset. A partial-month note is shown when the latest incident date falls before the end of its calendar month.

The **Run pipeline now** action starts the ETL process on the server hosting Streamlit. It does not run the ETL on the visitor's own computer. The dashboard refreshes its run status while the request is being processed. A pipeline lock prevents a second ETL process from overlapping an active run.

**Public deployment caution:** The current dashboard does not provide user authentication or HTTPS by itself. Anyone who can access an exposed dashboard with the run action may be able to request server-side pipeline execution. For a public demonstration, restrict access where practical. Before using this as a production service, put appropriate authentication and HTTPS in front of it and apply suitable rate/access controls. The pipeline lock prevents overlapping runs; it is not an access-control mechanism.

## Project structure

```text
crime-trends/
├── app.py                    # Streamlit dashboard and run request
├── run_pipeline.py           # Coordinates extraction -> validation -> transformation
├── extract.py                # Socrata extraction Prefect task
├── validate.py               # Data validation Prefect task
├── transform.py              # Data transformation Prefect task
├── check_postgres.py         # Separate PostgreSQL connectivity check
├── requirements.txt          # Python dependencies, including Matplotlib
├── compose.yaml              # Optional PostgreSQL service
├── .env.example              # Safe configuration template
├── .gitignore                # Excludes local secrets and generated files
├── README.md
├── PROJECT_NOTES.md
├── .streamlit/
│   └── config.toml
├── documentation/            # Supporting project documentation
├── tests/                    # Test directory
├── output/
│   └── .gitkeep
└── logs/
    └── .gitkeep
```

The actual contents of `documentation/` and `tests/` may vary as the project develops. Generated output files and logs are intentionally excluded from version control; the folders are retained with `.gitkeep` files.

## Data source

**Cambridge Open Data — Daily Police Log**

- Dataset page: https://data.cambridgema.gov/Public-Safety/Daily-Police-Log/3gki-wyrb/about_data
- Domain: `data.cambridgema.gov`
- Dataset ID: `3gki-wyrb`

The source is live. New records, updates, or changes to the source dataset can alter the results of later runs.

## Requirements

- Python 3.12 or 3.14. Python 3.12 is the recommended starting point for a new setup and matches the project's EC2 environment; the project has also been run locally with Python 3.14.
- Internet access for the Socrata API.
- Docker Desktop (Windows/macOS) or Docker Engine with the Compose plugin (Linux) **only if you want to run the optional PostgreSQL connectivity check**.
- A Socrata app token is recommended but not mandatory. Without one, the public API may apply stricter throttling.

All Python dependencies are listed in `requirements.txt`. This includes Matplotlib, which is used by the dashboard's visual styling.

The project retains the `sodapy` client to match the course-provided workflow. It is isolated in the extraction module so that the API access method can be replaced separately if required.

## Run locally

### 1. Get the project

Clone the repository or download and extract its ZIP archive. Open a terminal in the project root (`crime-trends/`).

git clone https://github.com/Kryysis/crime-trends.git

### 2. Create a Python virtual environment

**Windows PowerShell:**

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If you are using the Python 3.14 installation already available on your machine, replace `py -3.12` with `py -3.14`.

**macOS / Linux:**

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Use the same Python version when creating the environment and installing the dependencies.

### 3. Configure environment variables (optional for a basic ETL run)

Create a private `.env` file from the example.

**Windows PowerShell:**

```powershell
Copy-Item .env.example .env
notepad .env
```

**macOS / Linux:**

```bash
cp .env.example .env
```

Add a Socrata app token to `SOCRATA_APP_TOKEN` if you have one. A token is recommended for API access but is not required by the project code.

Do not commit `.env`, share it publicly, or place real credentials in `.env.example`. The `.gitignore` file is configured to exclude `.env`.

### 4. Run the ETL pipeline

With the virtual environment activated:

```bash
python run_pipeline.py
```

A successful run writes:

- `output/transformed_data.csv`
- `output/latest_summary.json`
- `logs/pipeline.log`

The live dataset may return a different number of records on a later run. Do not use a previous run's count as a fixed expected total.

### 5. Start the Streamlit dashboard

With the virtual environment activated:

```bash
python -m streamlit run app.py
```

Open the local URL printed in the terminal, normally:

```text
http://localhost:8501
```

The dashboard reads the latest run files from the same project directory. You can start a new run using the dashboard's **Run pipeline now** action, or run `python run_pipeline.py` in the terminal. Do not start a second run while one is already active.

### 6. Optional: run the PostgreSQL connectivity check

The ETL pipeline and Streamlit dashboard can run without Docker or PostgreSQL. The following steps are only needed to test the optional PostgreSQL service.

1. Install and start Docker Desktop or Docker Engine.
2. Configure the PostgreSQL values in `.env`. Change the example password before using the service.
3. Start PostgreSQL:

   ```bash
   docker compose up -d postgres
   docker compose ps
   ```

4. Check the connection:

   ```bash
   python check_postgres.py
   ```

The Compose file binds the host port to `127.0.0.1` (default host port `5433`) rather than exposing PostgreSQL to the public network. The service uses a named Docker volume to persist its database files.

No ETL records are inserted into PostgreSQL in this version. The connectivity script only verifies that the service can be reached.

To stop the container:

```bash
docker compose down
```

To remove the container **and permanently delete the persisted PostgreSQL volume**, use:

```bash
docker compose down -v
```

Only use `-v` when you intend to delete the database's stored data.

## Run on an AWS EC2 instance

The application can also run on a Linux EC2 instance. At a high level:

1. Install a supported Python version and create a virtual environment.
2. Clone or copy the project to the instance.
3. Install dependencies with `python -m pip install -r requirements.txt`.
4. Create the server-side `.env` file if a Socrata token or PostgreSQL configuration is needed.
5. Run `python run_pipeline.py` to generate the data and summary.
6. Start Streamlit as a persistent service, listening on the instance network interface.

For a dashboard reachable from outside the instance, configure the EC2 security group to allow inbound traffic on the chosen Streamlit port (the default is `8501`). Restrict SSH access (port `22`) to the administrator's current public IP. **Do not expose PostgreSQL ports (`5432` or `5433`) publicly.**

The current demonstration deployment uses HTTP on port `8501`; this is not a production security configuration. Use HTTPS and authentication before treating the dashboard as a production-facing service. Remember that the dashboard's run action executes the pipeline on the EC2 instance.

## Operational notes

- **Police log, not confirmed crime data:** Interpret the records as police-log entries/calls for service, not as a verified count of crimes.
- **Live source:** The source dataset can change. Extraction counts and date coverage can therefore change between runs.
- **Extraction order:** The extractor requests `:id ASC` ordering to reduce inconsistent page results. It does not freeze the dataset while a run is in progress.
- **Duplicate handling:** Duplicate IDs are removed during transformation as a data-quality safeguard.
- **Invalid IDs:** IDs that fail numeric validation are excluded from the transformed output.
- **Date fields:** Derived year/month/day/hour/minute/second values come from the source `date_time` field. Dashboard coverage is based on incident date-time, not the source's `last_updated` field.
- **Missing descriptive fields:** Subtype and description can be missing in source records; these are reported in the run metrics rather than automatically treated as invalid IDs.
- **Concurrency:** A lock file prevents overlapping ETL runs from writing to the same outputs. If a run is forcibly terminated and `output/pipeline.lock` remains, first verify that no pipeline process is still active. Only then remove the stale lock file.
- **PostgreSQL boundary:** PostgreSQL is configured for connectivity testing only. No table is created and no transformed records are loaded into it.
- **Orchestration boundary:** The stages are Prefect tasks, but this project does not currently deploy a Prefect flow or schedule recurring runs.

## Troubleshooting

- **`py -3.12` is not found:** Install Python 3.12, or use a Python version already installed (for example, `py -3.14`) and create the virtual environment with that version.
- **PowerShell blocks virtual-environment activation:** Run `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` in the current PowerShell window, then activate the environment again.
- **`streamlit` command is not recognized:** Activate `.venv` and use `python -m streamlit run app.py`.
- **A Python package is missing:** Confirm the virtual environment is active, then run `python -m pip install -r requirements.txt`.
- **Docker connection error:** Start Docker Desktop or Docker Engine and wait until its engine is ready. Docker is only needed for the optional PostgreSQL check.
- **PostgreSQL port conflict:** Change `POSTGRES_PORT` in `.env` to an unused host port, then recreate/restart the service and rerun `python check_postgres.py`.
- **Socrata throttling or request failures:** Configure a valid `SOCRATA_APP_TOKEN` in `.env` and retry. The source is external, so availability and rate limits are outside this project's control.
- **A previous pipeline run is active:** Wait for it to finish. Do not remove the lock file while a pipeline process is running.

## Project status

This is a course project focused on the ETL workflow and its supporting dashboard. The current implementation demonstrates extraction, validation, transformation, file-based output, run monitoring, and descriptive analytics. PostgreSQL loading, deployed Prefect orchestration, recurring scheduling, and production-grade dashboard security remain possible future extensions.
