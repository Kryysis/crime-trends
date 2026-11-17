"""Crime Trends dashboard with public ETL execution and analytics."""
#Divyam
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_ROOT / "output"
SUMMARY_PATH = OUTPUT_DIR / "latest_summary.json"
CSV_PATH = OUTPUT_DIR / "transformed_data.csv"
LOCK_PATH = OUTPUT_DIR / "pipeline.lock"
WEB_TRIGGER_LOG = PROJECT_ROOT / "logs" / "web_trigger.log"
WEB_TRIGGER_GUARD = OUTPUT_DIR / "web_trigger.guard"
SOURCE_URL = "https://data.cambridgema.gov/Public-Safety/Daily-Police-Log/3gki-wyrb/about_data"

st.set_page_config(
    page_title="Crime Trends | ETL & Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
    <style>
      .block-container {max-width: 1220px; padding-top: 1.8rem; padding-bottom: 3rem;}
      .project-subtitle {font-size: 1rem; opacity: .75; margin-top: -.35rem;}
      div[data-testid="stMetric"] {border: 1px solid rgba(128,128,128,.22); padding: .8rem; border-radius: .7rem;}
      @media (max-width: 700px) {
        .block-container {padding-left: 1rem; padding-right: 1rem;}
      }
    </style>
    """,
    unsafe_allow_html=True,
)


def load_latest_summary() -> dict[str, Any] | None:
    if not SUMMARY_PATH.exists():
        return None
    try:
        value = json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def display_count(value: Any) -> str:
    if value is None:
        return "—"
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError, OverflowError):
        return "—"


def stage_label(stage_name: str) -> str:
    return stage_name.replace("_", " ").title()


def launch_pipeline() -> tuple[bool, str]:
    """Start the existing ETL runner with a cross-session request guard."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    WEB_TRIGGER_LOG.parent.mkdir(parents=True, exist_ok=True)

    # O_EXCL makes the short critical section safe across separate Streamlit sessions.
    try:
        guard_fd = os.open(
            str(WEB_TRIGGER_GUARD),
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o600,
        )
    except FileExistsError:
        # Recover a guard left behind if the web process crashed during its short critical section.
        try:
            if time.time() - WEB_TRIGGER_GUARD.stat().st_mtime > 60:
                WEB_TRIGGER_GUARD.unlink(missing_ok=True)
                guard_fd = os.open(
                    str(WEB_TRIGGER_GUARD),
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o600,
                )
            else:
                return False, "Another run request is being processed. Please refresh in a moment."
        except (FileNotFoundError, FileExistsError, OSError):
            return False, "Another run request is being processed. Please refresh in a moment."
    except OSError as exc:
        return False, f"Could not establish the run guard: {type(exc).__name__}: {exc}"

    try:
        if LOCK_PATH.exists():
            return False, "A pipeline run is already in progress. Wait for it to finish, then refresh status."


        try:
            with WEB_TRIGGER_LOG.open("a", encoding="utf-8") as log_file:
                subprocess.Popen(
                    [sys.executable, str(PROJECT_ROOT / "run_pipeline.py")],
                    cwd=str(PROJECT_ROOT),
                    stdin=subprocess.DEVNULL,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    close_fds=True,
                )
        except OSError as exc:
            return False, f"Could not start the pipeline: {type(exc).__name__}: {exc}"


        return True, "Pipeline started successfully. Refresh status to see its progress and results."
    finally:
        try:
            os.close(guard_fd)
        except OSError:
            pass
        try:
            WEB_TRIGGER_GUARD.unlink(missing_ok=True)
        except OSError:
            pass


def load_processed_data() -> tuple[pd.DataFrame | None, str | None]:
    if not CSV_PATH.is_file():
        return None, "The transformed CSV is not available yet. Run the pipeline successfully first."
    try:
        data = pd.read_csv(CSV_PATH, low_memory=False)
    except (OSError, pd.errors.ParserError, UnicodeDecodeError, pd.errors.EmptyDataError) as exc:
        return None, f"Could not read the transformed CSV: {type(exc).__name__}: {exc}"
    if data.empty:
        return None, "The transformed CSV contains no records."
    return data, None


def parse_incident_datetimes(values: pd.Series) -> pd.Series:
    """Parse Cambridge floating timestamps as local wall time; normalize explicit offsets."""
    text_values = values.astype("string").str.strip()
    has_explicit_timezone = text_values.str.contains(
        r"(?:Z|[+-]\d{2}:?\d{2})$", case=False, na=False
    ).any()
    if has_explicit_timezone:
        parsed = pd.to_datetime(text_values, errors="coerce", utc=True)
        return parsed.dt.tz_convert("America/New_York").dt.tz_localize(None)
    return pd.to_datetime(text_values, errors="coerce", format="mixed")


def clean_category(series: pd.Series) -> pd.Series:
    cleaned = series.astype("string").str.strip()
    return cleaned.mask(cleaned.eq("") | cleaned.str.lower().isin(["nan", "none", "null"]))


def render_analytics(data: pd.DataFrame) -> None:
    st.markdown("## Incident analytics")
    st.caption(
        "Charts count police log entries, not necessarily confirmed crimes. "
        "The Cambridge Daily Police Log includes both criminal and non-criminal calls for service."
    )

    chart_data = data.copy()
    has_datetime = "date_time" in chart_data.columns
    if has_datetime:
        chart_data["_incident_datetime"] = parse_incident_datetimes(chart_data["date_time"])
        valid_datetime = chart_data["_incident_datetime"].notna()
    else:
        valid_datetime = pd.Series(False, index=chart_data.index)

    if valid_datetime.any():
        timed_data = chart_data.loc[valid_datetime].copy()
        min_date = timed_data["_incident_datetime"].min().date()
        max_date = timed_data["_incident_datetime"].max().date()
        st.caption(
            f"Incident-date coverage in the loaded dataset: "
            f"{min_date:%d %b %Y} – {max_date:%d %b %Y}."
        )

        filter_col_1, filter_col_2 = st.columns([2, 1])
        with filter_col_1:
            selected_dates = st.date_input(
                "Filter by incident date",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
                key="analytics_date_range",
            )
        with filter_col_2:
            available_types: list[str] = []
            if "type" in timed_data.columns:
                available_types = sorted(clean_category(timed_data["type"]).dropna().unique().tolist())
            selected_type = st.selectbox(
                "Report type",
                options=["All types", *available_types],
                key="analytics_report_type",
            )

        if isinstance(selected_dates, (tuple, list)) and len(selected_dates) == 2:
            start_date, end_date = selected_dates
            if start_date is None or end_date is None:
                start_date, end_date = min_date, max_date
        elif isinstance(selected_dates, date):
            start_date = end_date = selected_dates
        else:
            start_date, end_date = min_date, max_date

        incident_dates = timed_data["_incident_datetime"].dt.date
        filtered = timed_data.loc[
            incident_dates.between(start_date, end_date)
        ].copy()
        if selected_type != "All types" and "type" in filtered.columns:
            filtered = filtered.loc[clean_category(filtered["type"]) == selected_type].copy()

        st.caption(
            f"Showing {len(filtered):,} entries from {start_date:%d %b %Y} "
            f"to {end_date:%d %b %Y}."
        )

        # 1. Monthly entry trend. Empty months within the selected range are shown as zero.
        st.markdown("### Monthly police log entries")
        if not filtered.empty:
            first_month = pd.Timestamp(start_date).to_period("M")
            last_month = pd.Timestamp(end_date).to_period("M")
            all_months = pd.period_range(first_month, last_month, freq="M")
            month_counts = filtered["_incident_datetime"].dt.to_period("M").value_counts()
            monthly = (
                month_counts.reindex(all_months, fill_value=0)
                .rename_axis("month")
                .rename("entries")
                .reset_index()
            )
            monthly["month"] = monthly["month"].dt.to_timestamp()
            st.line_chart(
                monthly,
                x="month",
                y="entries",
                x_label="Month",
                y_label="Police log entries",
                height=300,
                use_container_width=True,
            )

            latest_data_month = pd.Period(max_date, freq="M")
            latest_displayed_month = monthly["month"].max().to_period("M")
            latest_month_end = latest_data_month.end_time.date()
            if (
                latest_displayed_month == latest_data_month
                and max_date < latest_month_end
            ):
                st.caption(
                    f"Note: {latest_data_month.strftime('%B %Y')} is a partial month. "
                    f"The loaded dataset contains incident records through "
                    f"{max_date:%d %b %Y}; do not compare this count directly "
                    "with complete months."
                )
        else:
            st.info("No entries match the selected date and report-type filters.")

        chart_left, chart_right = st.columns(2)

        # 2. Most frequent subtypes.
        with chart_left:
            st.markdown("### Top 10 report subtypes")
            if "subtype" in filtered.columns:
                subtype_values = clean_category(filtered["subtype"]).dropna()
                top_subtypes = (
                    subtype_values.value_counts()
                    .head(10)
                    .rename_axis("subtype")
                    .rename("entries")
                    .reset_index()
                )
                if not top_subtypes.empty:
                    st.bar_chart(
                        top_subtypes,
                        x="subtype",
                        y="entries",
                        horizontal=True,
                        sort=False,
                        x_label="Subtype",
                        y_label="Police log entries",
                        height=340,
                        use_container_width=True,
                    )
                else:
                    st.info("No subtype values are available for this selection.")
            else:
                st.info("The transformed dataset does not contain a subtype column.")

        # 3. Entries by hour of day.
        with chart_right:
            st.markdown("### Entries by hour of day")
            hourly_counts = (
                filtered["_incident_datetime"].dt.hour.value_counts()
                .reindex(range(24), fill_value=0)
                .rename_axis("hour")
                .rename("entries")
                .reset_index()
            )
            if not filtered.empty:
                st.bar_chart(
                    hourly_counts,
                    x="hour",
                    y="entries",
                    x_label="Hour of day (0–23)",
                    y_label="Police log entries",
                    height=340,
                    use_container_width=True,
                )
            else:
                st.info("No hourly activity to display for this selection.")

        location_left, location_right = st.columns(2)

        # 4. Most frequently recorded locations.
        with location_left:
            st.markdown("### Top 10 reported locations")
            if "location" in filtered.columns:
                location_values = clean_category(filtered["location"]).dropna()
                location_values = location_values.loc[
                    ~location_values.str.lower().isin(["unknown", "n/a", "not available"])
                ]
                top_locations = (
                    location_values.value_counts()
                    .head(10)
                    .rename_axis("location")
                    .rename("entries")
                    .reset_index()
                )
                if not top_locations.empty:
                    st.bar_chart(
                        top_locations,
                        x="location",
                        y="entries",
                        horizontal=True,
                        sort=False,
                        x_label="Reported street / block",
                        y_label="Police log entries",
                        height=340,
                        use_container_width=True,
                    )
                    st.caption("Locations are published at street or block level, not as precise addresses.")
                else:
                    st.info("No usable location values are available for this selection.")
            else:
                st.info("The transformed dataset does not contain a location column.")

        # 5. Weekday-by-hour heatmap, derived from the incident timestamp.
        with location_right:
            st.markdown("### Weekday and hour activity")
            if not filtered.empty:
                weekday_order = [
                    "Monday", "Tuesday", "Wednesday", "Thursday",
                    "Friday", "Saturday", "Sunday",
                ]
                heat_source = filtered.assign(
                    _weekday=filtered["_incident_datetime"].dt.day_name(),
                    _hour=filtered["_incident_datetime"].dt.hour,
                )
                heatmap = pd.crosstab(heat_source["_weekday"], heat_source["_hour"])
                heatmap = heatmap.reindex(
                    index=weekday_order,
                    columns=range(24),
                    fill_value=0,
                )
                heatmap.columns = [f"{hour:02d}" for hour in heatmap.columns]
                heatmap.index.name = "Day"
                heatmap.columns.name = "Hour"
                styled_heatmap = heatmap.style.background_gradient(cmap="Blues").format("{:,.0f}")
                st.dataframe(styled_heatmap, use_container_width=True, height=340)
                st.caption("Darker cells indicate more police log entries.")
            else:
                st.info("No weekday/hour activity to display for this selection.")

        invalid_datetime_count = int(len(chart_data) - int(valid_datetime.sum()))
        if invalid_datetime_count:
            st.caption(
                f"{invalid_datetime_count:,} row(s) with missing or unparseable date/time "
                "were excluded from time-based charts."
            )
    else:
        st.warning(
            "Time-based charts are unavailable because the transformed dataset has no usable `date_time` values."
        )
        chart_data = chart_data.copy()
        chart_left, chart_right = st.columns(2)
        with chart_left:
            st.markdown("### Top 10 report subtypes")
            if "subtype" in chart_data.columns:
                subtype_values = clean_category(chart_data["subtype"]).dropna()
                top_subtypes = subtype_values.value_counts().head(10).rename_axis("subtype").rename("entries").reset_index()
                if not top_subtypes.empty:
                    st.bar_chart(
                        top_subtypes,
                        x="subtype",
                        y="entries",
                        horizontal=True,
                        sort=False,
                        x_label="Subtype",
                        y_label="Police log entries",
                        use_container_width=True,
                    )
        with chart_right:
            st.markdown("### Top 10 reported locations")
            if "location" in chart_data.columns:
                location_values = clean_category(chart_data["location"]).dropna()
                top_locations = location_values.value_counts().head(10).rename_axis("location").rename("entries").reset_index()
                if not top_locations.empty:
                    st.bar_chart(
                        top_locations,
                        x="location",
                        y="entries",
                        horizontal=True,
                        sort=False,
                        x_label="Reported street / block",
                        y_label="Police log entries",
                        use_container_width=True,
                    )


with st.sidebar:
    st.markdown("### About this project")
    st.write(
        "A cloud-hosted ETL pipeline that retrieves, validates and transforms "
        "public Cambridge Police Department incident records."
    )
    st.link_button("Open source dataset", SOURCE_URL, use_container_width=True)
    st.caption(
        "The dashboard is publicly accessible. "
    )

st.title("Crime Trends")
st.markdown(
    '<div class="project-subtitle">Cambridge Police Department · ETL Pipeline Monitor & Analytics</div>',
    unsafe_allow_html=True,
)
st.write(
    "Explore patterns in public police log entries and monitor the latest data pipeline execution. "
)

@st.fragment(run_every="1s")
def render_live_status() -> None:
    """Refresh execution status without rerunning analytics."""
    summary_live = load_latest_summary()
    is_locked = LOCK_PATH.exists()
    live_status = str((summary_live or {}).get("status", "unknown")).lower()
    live_run_id = (summary_live or {}).get("run_id")

    # If a run requested from this dashboard has reached a terminal state, rerun
    # the whole app once so the newly generated analytics and CSV preview appear.
    requested_base_run_id = st.session_state.get("_requested_base_run_id")
    if (
        requested_base_run_id is not None
        and live_run_id != requested_base_run_id
        and live_status in {"success", "failed"}
        and not is_locked
    ):
        st.session_state.pop("_requested_base_run_id", None)
        st.session_state["_previous_execution_state"] = (live_run_id, live_status, is_locked)
        st.rerun(scope="app")

    # Also notice completion of runs started outside the public dashboard.
    current_execution_state = (live_run_id, live_status, is_locked)
    previous_execution_state = st.session_state.get("_previous_execution_state")
    if previous_execution_state is not None:
        previous_run_id, previous_status, previous_locked = previous_execution_state
        finished_externally = (
            live_run_id == previous_run_id
            and previous_status == "running"
            and live_status in {"success", "failed"}
            and not is_locked
        ) or (
            previous_locked
            and not is_locked
            and live_status in {"success", "failed"}
        )
        if finished_externally:
            st.session_state["_previous_execution_state"] = current_execution_state
            st.rerun(scope="app")
    st.session_state["_previous_execution_state"] = current_execution_state

    button_col, info_col = st.columns([1, 4])
    with button_col:
        if st.button("↻ Refresh status", use_container_width=True, key="refresh_live_status"):
            st.rerun(scope="app")
    with info_col:
        if is_locked:
            st.warning("A pipeline run is currently in progress. This panel refreshes automatically.")

    run_disabled = is_locked
    if st.button(
        "▶ Run pipeline now",
        type="primary",
        disabled=run_disabled,
        use_container_width=True,
        key="run_pipeline_live",
    ):
        started, message = launch_pipeline()
        if started:
            st.session_state["_requested_base_run_id"] = live_run_id
            st.success(message)
        else:
            st.warning(message)

    if is_locked:
        st.caption("Pipeline execution is in progress. Stages and metrics below update automatically every second.")
    else:
        st.caption("Live status refreshes automatically every second.")

    if summary_live is None:
        st.info("No pipeline execution has been recorded yet. Start the pipeline using the button above.")
        st.markdown("#### Pipeline stages")
        st.write("1. Extraction — waiting for first run")
        st.write("2. Validation — waiting for first run")
        st.write("3. Transformation — waiting for first run")
        st.link_button("View Cambridge Open Data source", SOURCE_URL)
        return

    live_status = str(summary_live.get("status", "unknown")).lower()
    if is_locked:
        st.warning("The latest pipeline execution is currently running.")
    elif live_status == "success":
        st.success("Latest pipeline run completed successfully.")
    elif live_status == "running":
        st.warning("A pipeline run is currently in progress. This panel refreshes automatically.")
    elif live_status == "failed":
        st.error("The latest pipeline run failed. The details below show where it stopped.")
    else:
        st.info(f"Latest pipeline status: {live_status}")

    live_metrics = summary_live.get("metrics") or {}
    metric_columns = st.columns(4)
    metric_specs = [
        ("Records extracted", live_metrics.get("records_extracted")),
        ("Duplicate IDs detected", live_metrics.get("duplicate_ids_detected")),
        ("Invalid IDs removed", live_metrics.get("invalid_ids_detected")),
        ("Records transformed", live_metrics.get("records_transformed")),
    ]
    for column, (label, value) in zip(metric_columns, metric_specs):
        column.metric(label, display_count(value))

    st.markdown("### Pipeline stages")
    stages = summary_live.get("stages") or {}
    stage_columns = st.columns(3)
    for column, stage_name in zip(stage_columns, ["extraction", "validation", "transformation"]):
        stage_info = stages.get(stage_name) or {}
        stage_status = str(stage_info.get("status", "pending")).lower()
        with column:
            st.markdown(f"**{stage_label(stage_name)}**")
            if stage_status == "passed":
                st.success("Passed")
            elif stage_status == "running":
                st.warning("Running")
            elif stage_status == "failed":
                st.error("Failed")
            else:
                st.info("Pending")

    st.markdown("### Latest execution")
    details_left, details_right = st.columns(2)
    with details_left:
        st.write(f"**Dataset ID:** `{summary_live.get('dataset_id', '3gki-wyrb')}`")
        st.write(f"**Started:** {summary_live.get('started_at') or '—'}")
        st.write(f"**Completed:** {summary_live.get('completed_at') or '—'}")
    with details_right:
        duration = summary_live.get("duration_seconds")
        st.write(f"**Duration:** {str(duration) + ' seconds' if duration is not None else '—'}")
        st.write(f"**Run ID:** `{summary_live.get('run_id', '—')}`")
        st.write(f"**Raw duplicate rows:** {display_count(live_metrics.get('raw_duplicate_rows'))}")

    with st.expander("Data-quality details"):
        st.write("Missing optional fields in the extracted data")
        st.write(f"- `subtype`: {display_count(live_metrics.get('missing_subtype'))}")
        st.write(f"- `description`: {display_count(live_metrics.get('missing_description'))}")
        st.caption(
            "Missing values in optional descriptive fields are reported for transparency; "
            "required fields are checked by the validation stage."
        )

    live_error = summary_live.get("error")
    if live_error:
        st.markdown("### Failure details")
        st.error(f"{live_error.get('type', 'Error')}: {live_error.get('message', 'No error message was recorded.')}")


render_live_status()
summary = load_latest_summary()
if summary is None:
    st.stop()
status = str(summary.get("status", "unknown")).lower()

if status == "success" and CSV_PATH.is_file():
    processed_data, data_error = load_processed_data()
    if data_error:
        st.error(data_error)
    elif processed_data is not None:
        render_analytics(processed_data)

        st.markdown("### Processed dataset")
        st.caption(
            "A preview of the most recently transformed records. "
            "Download the CSV for the complete output."
        )
        preferred_columns = [
            "date_time", "id", "type", "subtype", "location",
            "year", "month", "day", "hour",
        ]
        visible_columns = [column for column in preferred_columns if column in processed_data.columns]
        preview_data = processed_data.copy()
        if "date_time" in preview_data.columns:
            preview_data["_preview_datetime"] = parse_incident_datetimes(
                preview_data["date_time"]
            )
            preview_data = preview_data.sort_values(
                "_preview_datetime",
                ascending=False,
                na_position="last",
                kind="stable",
            ).drop(columns="_preview_datetime")
        preview_source = preview_data[visible_columns] if visible_columns else preview_data
        preview = preview_source.head(15)
        st.dataframe(preview, use_container_width=True, hide_index=True)
        try:
            csv_bytes = CSV_PATH.read_bytes()
            st.download_button(
                label="Download transformed dataset (CSV)",
                data=csv_bytes,
                file_name="transformed_data.csv",
                mime="text/csv",
                use_container_width=True,
            )
        except OSError as exc:
            st.error(f"The transformed CSV could not be prepared for download: {type(exc).__name__}: {exc}")
elif status == "success":
    st.warning("The run summary indicates success, but the CSV output file is missing. Investigate the server output folder.")
else:
    st.caption("The analytics, dataset preview and CSV download become available after a successful pipeline run.")

st.divider()
st.caption(
    "Data source: Cambridge Open Data · Daily Police Log · "
    "Each row represents a police log entry or call for service, not necessarily a confirmed crime."
)
