"""
Backfill historical flight data.

Fetches complete UTC days from the OpenSky Network API and loads them into
RAW.RAW_FLIGHTS using the same client and loader modules as the Airflow DAG,
so everything downstream is transformed by the existing dbt models.

Run from the project root:
    docker compose exec airflow-webserver python -m include.backfill
"""

import logging
import time
from datetime import datetime, timedelta, timezone

import requests

from include.opensky_client import AIRPORTS, get_access_token, get_flights
from include.snowflake_loader import get_snowflake_connection, load_flights

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)

# Which days to fetch, counted back from today. Both ends are included.
NEWEST_DAY_AGO = 1
OLDEST_DAY_AGO = 8

PAUSE_BETWEEN_CALLS_SECONDS = 2
RATE_LIMIT_WAIT_SECONDS = 60
MAX_RETRIES = 3


def day_window(days_ago):
    """Return the day start plus the unix begin and end for that whole UTC day."""
    day = datetime.now(tz=timezone.utc) - timedelta(days=days_ago)
    day_start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1) - timedelta(seconds=1)
    return day_start, int(day_start.timestamp()), int(day_end.timestamp())


def fetch_with_retry(token, airport, begin, end, direction):
    """Call the API, backing off and retrying if OpenSky rate limits us."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return get_flights(token, airport, begin, end, direction)
        except requests.exceptions.HTTPError as err:
            if err.response is not None and err.response.status_code == 429:
                wait = RATE_LIMIT_WAIT_SECONDS * attempt
                logger.warning(
                    "Rate limited on %s %s. Waiting %ds before attempt %d.",
                    airport, direction, wait, attempt + 1,
                )
                time.sleep(wait)
            else:
                raise
    raise RuntimeError("Gave up after repeated rate limiting")


def fetch_day(token, begin, end):
    """Fetch arrivals and departures for every airport, pacing the calls."""
    flights = []
    for airport in AIRPORTS:
        for direction in ("arrival", "departure"):
            flights.extend(fetch_with_retry(token, airport, begin, end, direction))
            time.sleep(PAUSE_BETWEEN_CALLS_SECONDS)
    return flights


def main():
    conn = get_snowflake_connection()
    total = 0

    try:
        for days_ago in range(NEWEST_DAY_AGO, OLDEST_DAY_AGO + 1):
            day_start, begin, end = day_window(days_ago)
            label = day_start.strftime("%Y-%m-%d")
            logger.info("Fetching %s", label)

            # A fresh token per day: OpenSky tokens expire after 5 minutes.
            token = get_access_token()

            flights = fetch_day(token, begin, end)
            loaded = load_flights(conn, flights)
            total += loaded
            logger.info("Loaded %d rows for %s", loaded, label)
    finally:
        conn.close()

    logger.info("Backfill complete. %d rows loaded into RAW_FLIGHTS.", total)


if __name__ == "__main__":
    main()