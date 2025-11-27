-- Ensures that profit is always less than or equal to revenue in stg_fortune500

select
    *
from {{ ref('stg_fortune500') }}
where profits_m > revenues_m
