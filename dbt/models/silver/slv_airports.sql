with airports_seed as (
    select * from {{ ref('seed_airports') }}
)

select
    airport_icao,
    airport_name,
    city,
    country,
    latitude,
    longitude,
    timezone,
    utc_offset_hours,
    is_tracked
from airports_seed