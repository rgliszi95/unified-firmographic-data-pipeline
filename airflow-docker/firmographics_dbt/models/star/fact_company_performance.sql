
select
    {{ dbt_utils.generate_surrogate_key([ 'company_name','symbol' ]) }} as company_key,
    {{ dbt_utils.generate_surrogate_key([ 'company_name','headquarters_city','headquarters_state' ]) }} as location_key,
    {{ dbt_utils.generate_surrogate_key([ 'company_name','slug' ]) }} as fortune_metrics_key,
    
    assets_m,
    revenues_m,
    profits_m,
    market_value_m,
    revenue_pct_change,
    profit_pct_change,
    employees,
    last_updated
from  {{ ref('cr_company_complete') }}
{% if is_incremental() %}
    where last_updated > (select max(last_updated) from {{ this }})
{% endif %}
