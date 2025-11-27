
select
    {{ dbt_utils.generate_surrogate_key(['company_name','symbol']) }} as company_key,
    symbol,
    company_name,
    industry,
    sector,
    cik,
    founded_year
from {{ ref('cr_company_complete') }} 
