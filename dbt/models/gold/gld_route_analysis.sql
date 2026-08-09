with flights as (
    select * from {{ ref('slv_flights') }}
),

routes as (
    select
        departure_airport_icao,
        arrival_airport_icao,
        count(*)                            as total_flights,
        avg(flight_duration_minutes)        as avg_duration_min,
        min(flight_duration_minutes)        as min_duration_min,
        max(flight_duration_minutes)        as max_duration_min,
        count(distinct aircraft_icao24)     as unique_aircraft,
        count(distinct departed_at::date)   as days_with_service,
        min(departed_at::date)              as first_seen_date,
        max(departed_at::date)              as last_seen_date
    from flights
    where departure_airport_icao is not null
      and arrival_airport_icao is not null
    group by 1, 2
)

select
    r.*,
    dep.airport_name    as departure_airport_name,
    dep.city            as departure_city,
    dep.country         as departure_country,
    arr.airport_name    as arrival_airport_name,
    arr.city            as arrival_city,
    arr.country         as arrival_country
from routes r
left join {{ ref('slv_airports') }} dep
    on r.departure_airport_icao = dep.airport_icao
left join {{ ref('slv_airports') }} arr
    on r.arrival_airport_icao = arr.airport_icao