"""
OpenSky Flights DAG
-------------------
Daily pipeline that extracts flight arrivals and departures from the
OpenSky Network API for a set of global airports, then loads the raw
data into Snowflake for downstream dbt modelling.
"""

from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task

# ── Default args ────────────────────────────────────────────────────
default_args = {
    "owner": "suriyakc",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="opensky_flights_to_snowflake",
    default_args=default_args,
    description="Extract OpenSky flight data and load into Snowflake",
    schedule="@daily",
    start_date=datetime(2025, 3, 15),
    catchup=False,
    tags=["opensky", "snowflake", "raw"],
)
def opensky_flights_dag():

    @task()
    def extract_flights(**context):
        """Pull previous day arrivals & departures from OpenSky API."""
        import sys, os
        sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "include"))
        from opensky_client import get_access_token, fetch_all_airports

        # Previous full day: 00:00 → 23:59:59 UTC
        execution_date = context["logical_date"]
        day_start = execution_date.replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)
        day_end = day_start + timedelta(days=1) - timedelta(seconds=1)

        begin = int(day_start.timestamp())
        end = int(day_end.timestamp())

        token = get_access_token()
        flights = fetch_all_airports(token, begin, end)
        return flights

    @task()
    def load_to_snowflake(flights):
        """Load extracted flight data into Snowflake RAW_FLIGHTS table."""
        import sys, os
        sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "include"))
        from snowflake_loader import (
            get_snowflake_connection,
            create_raw_table_if_not_exists,
            load_flights,
        )

        conn = get_snowflake_connection()
        try:
            create_raw_table_if_not_exists(conn)
            count = load_flights(conn, flights)
        finally:
            conn.close()
        return count

    @task()
    def log_summary(count):
        """Log a summary of the ingestion run."""
        import logging
        logger = logging.getLogger(__name__)
        logger.info("Pipeline complete — %d flight records ingested into Snowflake.", count)

    # ── Task dependencies ───────────────────────────────────────────
    flights = extract_flights()
    count = load_to_snowflake(flights)
    log_summary(count)


opensky_flights_dag()