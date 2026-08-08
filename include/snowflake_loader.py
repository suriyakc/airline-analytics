import os
import logging
from datetime import datetime, timezone

import snowflake.connector

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS RAW_FLIGHTS (
    icao24                                VARCHAR,
    first_seen                            TIMESTAMP_NTZ,
    est_departure_airport                 VARCHAR,
    last_seen                             TIMESTAMP_NTZ,
    est_arrival_airport                   VARCHAR,
    callsign                              VARCHAR,
    est_departure_airport_horiz_distance  INT,
    est_departure_airport_vert_distance   INT,
    est_arrival_airport_horiz_distance    INT,
    est_arrival_airport_vert_distance     INT,
    departure_airport_candidates_count    INT,
    arrival_airport_candidates_count      INT,
    flight_direction                      VARCHAR,
    queried_airport                       VARCHAR,
    ingested_at                           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
"""

INSERT_SQL = """
INSERT INTO RAW_FLIGHTS (
    icao24, first_seen, est_departure_airport, last_seen,
    est_arrival_airport, callsign,
    est_departure_airport_horiz_distance, est_departure_airport_vert_distance,
    est_arrival_airport_horiz_distance, est_arrival_airport_vert_distance,
    departure_airport_candidates_count, arrival_airport_candidates_count,
    flight_direction, queried_airport, ingested_at
) VALUES (
    %(icao24)s, %(first_seen)s, %(est_departure_airport)s, %(last_seen)s,
    %(est_arrival_airport)s, %(callsign)s,
    %(est_departure_airport_horiz_distance)s, %(est_departure_airport_vert_distance)s,
    %(est_arrival_airport_horiz_distance)s, %(est_arrival_airport_vert_distance)s,
    %(departure_airport_candidates_count)s, %(arrival_airport_candidates_count)s,
    %(flight_direction)s, %(queried_airport)s, %(ingested_at)s
);
"""


def get_snowflake_connection():
    """Create a Snowflake connection using environment variables."""
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )


def create_raw_table_if_not_exists(conn):
    """Ensure the RAW_FLIGHTS table exists in Snowflake."""
    conn.cursor().execute(CREATE_TABLE_SQL)
    logger.info("RAW_FLIGHTS table ready.")


def _unix_to_timestamp(ts):
    """Convert a Unix timestamp (seconds) to a datetime, or None."""
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def load_flights(conn, flights):
    """
    Insert a list of flight dicts into Snowflake RAW_FLIGHTS.

    Each dict should have the keys returned by the OpenSky API plus
    'flight_direction' and 'queried_airport' added by the client.
    """
    if not flights:
        logger.info("No flights to load.")
        return 0

    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for f in flights:
        rows.append(
            {
                "icao24": f.get("icao24"),
                "first_seen": _unix_to_timestamp(f.get("firstSeen")),
                "est_departure_airport": f.get("estDepartureAirport"),
                "last_seen": _unix_to_timestamp(f.get("lastSeen")),
                "est_arrival_airport": f.get("estArrivalAirport"),
                "callsign": (f.get("callsign") or "").strip() or None,
                "est_departure_airport_horiz_distance": f.get(
                    "estDepartureAirportHorizDistance"
                ),
                "est_departure_airport_vert_distance": f.get(
                    "estDepartureAirportVertDistance"
                ),
                "est_arrival_airport_horiz_distance": f.get(
                    "estArrivalAirportHorizDistance"
                ),
                "est_arrival_airport_vert_distance": f.get(
                    "estArrivalAirportVertDistance"
                ),
                "departure_airport_candidates_count": f.get(
                    "departureAirportCandidatesCount"
                ),
                "arrival_airport_candidates_count": f.get(
                    "arrivalAirportCandidatesCount"
                ),
                "flight_direction": f.get("flight_direction"),
                "queried_airport": f.get("queried_airport"),
                "ingested_at": now,
            }
        )

    cursor = conn.cursor()
    cursor.executemany(INSERT_SQL, rows)
    logger.info("Loaded %d flight records into Snowflake.", len(rows))
    return len(rows)