import os
import logging
import requests

logger = logging.getLogger(__name__)

OPENSKY_AUTH_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
OPENSKY_API_BASE = "https://opensky-network.org/api"

AIRPORTS = ["KJFK", "EGLL", "LFPG", "RJTT", "OMDB", "YSSY"]


def get_access_token():
    """Exchange client credentials for an OAuth2 bearer token."""
    client_id = os.environ["OPENSKY_CLIENT_ID"]
    client_secret = os.environ["OPENSKY_CLIENT_SECRET"]

    response = requests.post(
        OPENSKY_AUTH_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    response.raise_for_status()
    token = response.json()["access_token"]
    logger.info("OpenSky access token obtained successfully.")
    return token


def get_flights(token, airport, begin, end, direction):
    """
    Fetch arrivals or departures for a single airport in a time window.

    Args:
        token: OAuth2 bearer token.
        airport: ICAO airport code (e.g. "KJFK").
        begin: Start of interval as Unix timestamp (seconds).
        end: End of interval as Unix timestamp (seconds).
        direction: "arrival" or "departure".

    Returns:
        List of flight dicts, or empty list if none found.
    """
    endpoint = f"{OPENSKY_API_BASE}/flights/{direction}"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"airport": airport, "begin": begin, "end": end}

    response = requests.get(endpoint, headers=headers, params=params, timeout=60)

    if response.status_code == 404:
        logger.info("No %s flights found for %s.", direction, airport)
        return []

    response.raise_for_status()
    flights = response.json()

    for flight in flights:
        flight["flight_direction"] = direction
        flight["queried_airport"] = airport

    logger.info(
        "Fetched %d %s flights for %s.", len(flights), direction, airport
    )
    return flights


def fetch_all_airports(token, begin, end, airports=None):
    """
    Fetch arrivals and departures for all configured airports.

    Returns:
        Combined list of flight dicts with direction and airport metadata.
    """
    if airports is None:
        airports = AIRPORTS

    all_flights = []
    for airport in airports:
        for direction in ("arrival", "departure"):
            flights = get_flights(token, airport, begin, end, direction)
            all_flights.extend(flights)

    logger.info("Total flights fetched across all airports: %d", len(all_flights))
    return all_flights