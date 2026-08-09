with flights as (
    select * from {{ ref('slv_flights') }}
),

arrivals as (
    select
        arrived_at::date                    as flight_date,
        arrival_airport_icao                as airport_icao,
        count(*)                            as total_arrivals,
        avg(flight_duration_minutes)        as avg_arrival_duration_min,
        min(flight_duration_minutes)        as min_arrival_duration_min,
        max(flight_duration_minutes)        as max_arrival_duration_min
    from flights
    where flight_direction = 'arrival'
      and arrival_airport_icao is not null
    group by 1, 2
),

departures as (
    select
        departed_at::date                   as flight_date,
        departure_airport_icao              as airport_icao,
        count(*)                            as total_departures,
        avg(flight_duration_minutes)        as avg_departure_duration_min
    from flights
    where flight_direction = 'departure'
      and departure_airport_icao is not null
    group by 1, 2
),

combined as (
    select
        coalesce(a.flight_date, d.flight_date)      as flight_date,
        coalesce(a.airport_icao, d.airport_icao)    as airport_icao,
        coalesce(a.total_arrivals, 0)               as total_arrivals,
        coalesce(d.total_departures, 0)             as total_departures,
        coalesce(a.total_arrivals, 0)
            + coalesce(d.total_departures, 0)       as total_movements,
        a.avg_arrival_duration_min,
        a.min_arrival_duration_min,
        a.max_arrival_duration_min,
        d.avg_departure_duration_min
    from arrivals a
    full outer join departures d
        on a.flight_date = d.flight_date
        and a.airport_icao = d.airport_icao
)

select
    c.*,
    ap.airport_name,
    ap.city,
    ap.country
from combined c
left join {{ ref('slv_airports') }} ap
    on c.airport_icao = ap.airport_icao