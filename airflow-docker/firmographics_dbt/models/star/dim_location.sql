select
    location_key,
    headquarters_city,
    headquarters_state,
    dbt_valid_from as valid_from
from {{ ref('company_location_snapshot') }}
where dbt_valid_to is null