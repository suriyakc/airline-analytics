with flights as (
    select * from {{ ref('slv_flights') }}
),

airline_stats as (
    select
        f.callsign_prefix,
        f.queried_airport                               as airport_icao,
        f.flight_direction,
        count(*)                                        as total_flights,
        count(distinct f.aircraft_icao24)               as unique_aircraft,
        count(distinct departed_at::date)               as days_active,
        round(avg(f.flight_duration_minutes), 1)        as avg_duration_min,
        min(f.departed_at::date)                        as first_seen_date,
        max(f.departed_at::date)                        as last_seen_date
    from flights f
    where f.callsign_prefix is not null
      and f.callsign_prefix != ''
    group by 1, 2, 3
)

select
    s.*,
    al.airline_name,
    al.airline_iata,
    al.airline_country,
    al.alliance,
    ap.airport_name,
    ap.city,
    ap.country as airport_country
from airline_stats s
left join {{ ref('slv_airlines') }} al
    on s.callsign_prefix = al.callsign_prefix
left join {{ ref('slv_airports') }} ap
    on s.airport_icao = ap.airport_icao