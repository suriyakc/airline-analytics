with raw as (
    select * from {{ ref('brz_raw_flights') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by icao24, first_seen, last_seen, flight_direction
            order by ingested_at desc
        ) as _row_num
    from raw
    where
        -- Remove records with null airports on both sides
        (est_departure_airport is not null or est_arrival_airport is not null)
        -- Remove clearly invalid durations
        and first_seen <= last_seen
),

cleaned as (
    select
        -- Identifiers
        {{ dbt_utils.generate_surrogate_key([
            'icao24', 'first_seen', 'last_seen', 'flight_direction'
        ]) }}                                           as flight_id,
        icao24                                          as aircraft_icao24,
        trim(callsign)                                  as callsign,

        -- Airports
        est_departure_airport                           as departure_airport_icao,
        est_arrival_airport                             as arrival_airport_icao,

        -- Timestamps
        first_seen                                      as departed_at,
        last_seen                                       as arrived_at,
        timediff('minute', first_seen, last_seen)       as flight_duration_minutes,

        -- Distance metrics
        est_departure_airport_horiz_distance            as departure_horiz_distance_m,
        est_departure_airport_vert_distance             as departure_vert_distance_m,
        est_arrival_airport_horiz_distance              as arrival_horiz_distance_m,
        est_arrival_airport_vert_distance               as arrival_vert_distance_m,

        -- Confidence
        departure_airport_candidates_count,
        arrival_airport_candidates_count,

        -- Airline lookup key
        upper(left(trim(callsign), 3))                  as callsign_prefix,

        -- Metadata
        flight_direction,
        queried_airport,
        ingested_at
    from deduplicated
    where _row_num = 1
)

select * from cleaned