with airlines_seed as (
    select * from {{ ref('seed_airlines') }}
)

select
    callsign_prefix,
    airline_name,
    airline_iata,
    country        as airline_country,
    alliance
from airlines_seed