# Data Pipeline to Monitor Local Crime Trends

**Student:** Divyam
**Roll number:** 8026340009
**Programme:** M.Tech Artificial Intelligence
**Semester:** 1st Semester
**Group:** 1
**Course:** PAI102 — Data Engineering
**Institution:** Thapar Institute of Engineering & Technology
**Source:** Cambridge Open Data — Daily Police Log (`3gki-wyrb`)

A modular Python ETL pipeline that extracts public Cambridge Police Department daily-log records, validates and transforms them, publishes a CSV and run summary, and exposes pipeline monitoring and descriptive analytics through a Streamlit dashboard. The dashboard can run locally or be hosted on AWS EC2.

> **Interpretation:** The source contains police-log entries and calls for service, not only confirmed crimes. Dashboard metrics must be interpreted as police log entries/reports, not counts of proven crimes.

## 1. Features

- Extracts records from the Socrata API using `sodapy`.
- Validates the expected schema, required fields, timestamps and incident IDs.
- Removes duplicate valid incident IDs and invalid IDs.
- Derives year, month, day, hour, minute and second fields from `date_time`.
- Uses Prefect task decorators for extraction, validation and transformation.
- Writes `output/transformed_data.csv`, `output/latest_summary.json` and `logs/pipeline.log`.
- Provides a Streamlit execution monitor, live one-second status/cooldown refresh, public run control with a five-minute shared cooldown, descriptive charts, data preview and CSV download.
- Includes PostgreSQL 17 through Docker Compose and a separate connectivity-check script.
- Deploys the Streamlit app as a persistent systemd service on AWS EC2.

## 2. Architecture

```text
Cambridge Open Data / Socrata API
              |
              v
          extract.py
              |
              v
          validate.py
              |
              v
          transform.py
              |
              v
    output/transformed_data.csv
              |
              +----> app.py (Streamlit dashboard)
                         |-- latest_summary.json
                         |-- analytics and filtered charts
                         |-- 15-row preview
                         `-- CSV download
```

`run_pipeline.py` coordinates the Prefect tasks, updates the JSON summary during execution, manages `output/pipeline.lock`, and publishes the transformed CSV. The Streamlit app reads the generated files and can launch `run_pipeline.py` on the host where it is running.

## 3. Project structure

```text
crime-trends/
├── app.py                   # Streamlit dashboard and public run control
├── run_pipeline.py          # ETL runner and execution-state management
├── extract.py               # Socrata extraction Prefect task
├── validate.py              # Schema, required-field and timestamp validation
├── transform.py             # Deduplication, invalid-ID removal and time features
├── check_postgres.py         # PostgreSQL connectivity test only
├── requirements.txt
├── compose.yaml              # PostgreSQL 17 container
├── .env.example              # Configuration template
├── .env                      # Private configuration; never commit or share
├── .streamlit/config.toml
├── output/                   # CSV, summary JSON and run lock (generated)
├── logs/                     # ETL and web-trigger logs (generated)
└── tests/                    # Reserved; no maintained automated suite yet
```

## 4. Data source and expected fields

- **Dataset:** Cambridge Police Department Daily Police Log
- **Dataset ID:** `3gki-wyrb`
- **Domain:** `data.cambridgema.gov`
- **Dataset page:** <https://data.cambridgema.gov/Public-Safety/Daily-Police-Log/3gki-wyrb/about_data>

Expected source fields: `date_time`, `id`, `type`, `subtype`, `location`, `last_updated`, `description`.

Required non-null fields: `date_time`, `id`, `type`, `location`.

## 5. ETL stages

### Extraction

`extract_data()` creates a Socrata client, retrieves all available records for the dataset and converts them into a pandas DataFrame. A Socrata app token is recommended but optional. Extraction is configured for three retries with a 10-second retry delay.

### Validation

`validate_data()` checks that the expected columns are present and no unexpected columns exist, verifies required values, parses timestamps for validity, and converts IDs to numeric values. Missing required IDs fail validation; malformed non-empty IDs become `NaN` and are removed during transformation.

### Transformation

`transform_data()` keeps the first row for each valid incident ID, removes rows whose IDs are `NaN`, parses `date_time`, and derives `year`, `month`, `day`, `hour`, `minute` and `second`.

### Run lifecycle

The runner creates an exclusive lock file, writes status updates to JSON before/after stages, writes the CSV through a temporary file and atomic replacement, records success/failure and duration, and removes the lock in cleanup.

## 6. Outputs

| Path | Purpose |
|---|---|
| `output/transformed_data.csv` | Transformed dataset after a successful run |
| `output/latest_summary.json` | Latest run ID, status, stage states, timestamps, metrics and error details |
| `output/pipeline.lock` | Prevents overlapping ETL runs |
| `output/last_web_trigger.txt` | Timestamp for the shared public-trigger cooldown |
| `logs/pipeline.log` | ETL execution log |
| `logs/web_trigger.log` | Output/errors from runs launched by Streamlit |

The JSON summary includes counts for extracted rows, fully duplicated raw rows, repeated valid IDs, invalid IDs, transformed rows and missing optional descriptive fields.

## 7. Dashboard analytics

The dashboard provides:

1. Monthly police log entry trend.
2. Top 10 report subtypes.
3. Entry counts by hour of day.
4. Top 10 reported locations.
5. Weekday-by-hour activity heatmap.

Users can filter by incident date range and report type. The dashboard displays up to 15 rows in the preview and allows the full CSV to be downloaded. The live execution panel refreshes every second; the analytics are not recalculated every second.

**Dependency note:** the heatmap uses `pandas.Styler.background_gradient`, which requires Matplotlib. The supplied `requirements.txt` currently does not list Matplotlib, so install it explicitly (`python -m pip install matplotlib`) or add it to `requirements.txt` for a fully reproducible environment.

## 8. Local setup (Windows PowerShell)

Prerequisites: Python, Docker Desktop, internet access, and the project folder.

```powershell
cd "C:\Users\<your-user>\Projects\DE\crime-trends"
py -3.14 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m pip install matplotlib
Copy-Item .env.example .env
notepad .env
```

Set a strong `POSTGRES_PASSWORD` in `.env`. Add `SOCRATA_APP_TOKEN` if available. Do not commit or share `.env`.

Start and verify PostgreSQL:

```powershell
docker compose up -d
docker ps
python check_postgres.py
```

Run the ETL manually:

```powershell
python run_pipeline.py
```

Start the dashboard (in another activated terminal):

```powershell
python -m streamlit run app.py
```

Open <http://localhost:8501>.

The dashboard also has a **Run pipeline now** button, so a run can be started from the local interface. Local execution uses the laptop and internet connection, not AWS compute.

Stop Streamlit with `Ctrl+C`. Stop PostgreSQL with:

```powershell
docker compose down
```

This keeps the named volume. `docker compose down -v` also deletes the persisted database volume and should only be used when that deletion is intended.

## 9. AWS EC2 deployment

The demonstrated deployment uses Ubuntu 24.04 on EC2. The project is under `~/crime-trends`; the Python environment is `.venv`; PostgreSQL is in Docker; Streamlit is managed by systemd.

After connecting through SSH, a fresh deployment requires Python/venv, Docker Engine and Docker Compose, project files, a server-side `.env`, dependency installation (including Matplotlib), PostgreSQL startup, and a connectivity check.

```bash
cd ~/crime-trends
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m pip install matplotlib
sudo docker compose up -d
docker ps
python check_postgres.py
```

The deployed systemd service is `crime-trends.service` and runs Streamlit on `0.0.0.0:8501`:

```bash
sudo systemctl status crime-trends --no-pager
sudo systemctl restart crime-trends
sudo journalctl -u crime-trends -f
```

Current observed demonstration endpoint (16 November 2026): <http://3.24.179.200:8501>. The EC2 public IP may change after stop/start unless an Elastic IP is attached. Confirm the current public address in the EC2 console before a demonstration.

### Security-group rules used for the demo

- TCP 22 (SSH): restrict source to the administrator's current public IP `/32`.
- TCP 8501 (Streamlit): opened to intended viewers for the academic demo.
- Do not add a public PostgreSQL inbound rule. Compose binds PostgreSQL to `127.0.0.1` on the host.

## 10. Updating code on AWS

1. Edit and test the file locally.
2. From Windows PowerShell, upload the changed file with SCP (use your own key path and current EC2 public IP):

```powershell
scp -i "C:\path\to\crime-trends-key.pem" `
  "C:\Users\<your-user>\Projects\DE\crime-trends\app.py" `
  ubuntu@<EC2_PUBLIC_IP>:~/crime-trends/app.py
```

3. Connect to EC2 and restart Streamlit after changing `app.py`:

```bash
sudo systemctl restart crime-trends
sudo systemctl status crime-trends --no-pager
```

4. Open the AWS dashboard and hard-refresh the browser (`Ctrl+F5`) if needed. Install changed Python dependencies in the EC2 `.venv` before restarting. Keep the server `.env` separate; never overwrite it with the local `.env`.

## 11. Security and operational notes

- The deployed dashboard uses HTTP, not HTTPS, and does not implement login authentication.
- Anyone who can reach the public link can view the dashboard and request pipeline execution.
- A shared five-minute cooldown, a short-lived dashboard request guard and the ETL lock reduce repeated/overlapping requests; these are not authentication controls.
- Keep `.env` and the EC2 `.pem` key private.
- Restrict SSH to the current administrator IP `/32`; the public IP of a laptop can change when switching networks.
- Keep PostgreSQL private; do not expose port 5432/5433 publicly.
- This small EC2 instance has limited RAM. Swap was enabled after an out-of-memory event; avoid concurrent runs and monitor memory.

## 12. Important scope boundaries

This version does **not** load data into PostgreSQL. PostgreSQL is only started and checked for connectivity. It also does not include a Metabase dashboard, Prefect flow deployment, Prefect Cloud, or a recurring schedule. Pipeline runs are initiated manually through the CLI or dashboard button.

## 13. Troubleshooting

| Problem | Resolution |
|---|---|
| `streamlit` not recognized | Activate `.venv`; use `python -m streamlit run app.py`. |
| Matplotlib error | Run `python -m pip install matplotlib` in the active `.venv`. |
| SSH timeout | Find the public IP with `(Invoke-RestMethod https://api.ipify.org)` and update inbound SSH source to `<IP>/32`. `ipconfig` only shows local/private IP details. |
| PostgreSQL check fails | Start Docker Desktop/Engine; run `docker compose ps`; verify `.env` and port 5433. |
| Socrata throttling | Configure `SOCRATA_APP_TOKEN` in `.env` and retry. |
| Stale lock | First confirm no `run_pipeline.py` process is active; only then remove `output/pipeline.lock`. |
| EC2 OOM | Check `free -h` and `journalctl -k`; avoid overlapping runs or use a larger instance if needed. |
| Dashboard not updating | Restart `crime-trends.service` and hard-refresh the browser. |

## 14. Future work

Potential extensions include PostgreSQL table loading, a formal Prefect flow/deployment and schedule, automated tests, dependency pinning, run-history storage, authenticated HTTPS access, and alerts for schema changes, API failures and stale data.
