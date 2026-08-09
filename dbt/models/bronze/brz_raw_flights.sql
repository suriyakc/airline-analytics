with source as (
    select * from {{ source('raw', 'raw_flights') }}
)

select
    icao24,
    first_seen,
    est_departure_airport,
    last_seen,
    est_arrival_airport,
    callsign,
    est_departure_airport_horiz_distance,
    est_departure_airport_vert_distance,
    est_arrival_airport_horiz_distance,
    est_arrival_airport_vert_distance,
    departure_airport_candidates_count,
    arrival_airport_candidates_count,
    flight_direction,
    queried_airport,
    ingested_at
from source