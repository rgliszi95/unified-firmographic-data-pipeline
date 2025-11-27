

with combined as (
    select
        f.ingested_at as last_updated,
        f.COMPANY_NAME,
        f.COMPANY_ORDER ,
        f.COMPANY_RANK ,
        f.SLUG,
        f.ASSETS_M,
        f.REVENUES_M,
        f.PROFITS_M,
        f.MARKET_VALUE_M,
        f.EMPLOYEES,
        f.REVENUE_PCT_CHANGE,
        f.PROFIT_PCT_CHANGE,
        f.HEADQUARTERS_CITY,
        f.HEADQUARTERS_STATE,
        f.INDUSTRY,
        f.SECTOR,
        f.IS_BEST_COMPANY,
        f.IS_CHANGE_THE_WORLD,
        f.DROPPED_IN_RANK,
        f.IS_FUTURE_50,
        f.IS_GLOBAL_500,
        f.IS_PROFITABLE,
        f.IS_NEWCOMER,
        f.HAS_FEMALE_CEO,
        f.FOUNDER_IS_CEO,
        f.IS_FASTEST_GROWING,
        f.IS_MOST_ADMIRED,
        f.CHANGE_RANK_500,
        f.CHANGE_RANK_1000,
        s.SYMBOL,
        s.CIK,
        s.DATE_ADDED,
        s.FOUNDED_YEAR,
        s.GICS_SECTOR,
        s.GICS_SUB_INDUSTRY
    from {{ ref('stg_fortune500') }} f
    join {{ ref('stg_wiki_sp500') }} s on f.company_name=s.company_name
    {% if is_incremental() %}
        where s.ingested_at > (select max(last_updated) from {{ this }})
    {% endif %}
),

ranked as (
    select
        *,
        row_number() over (partition by company_name order by last_updated desc) as rn
    from combined
)

select
    last_updated,
    COMPANY_NAME,
    COMPANY_ORDER,
    COMPANY_RANK,
    SLUG,
    ASSETS_M,
    REVENUES_M,
    PROFITS_M,
    MARKET_VALUE_M,
    EMPLOYEES,
    REVENUE_PCT_CHANGE,
    PROFIT_PCT_CHANGE,
    HEADQUARTERS_CITY,
    HEADQUARTERS_STATE,
    INDUSTRY,
    SECTOR,
    IS_BEST_COMPANY,
    IS_CHANGE_THE_WORLD,
    DROPPED_IN_RANK,
    IS_FUTURE_50,
    IS_GLOBAL_500,
    IS_PROFITABLE,
    IS_NEWCOMER,
    HAS_FEMALE_CEO,
    FOUNDER_IS_CEO,
    IS_FASTEST_GROWING,
    IS_MOST_ADMIRED,
    CHANGE_RANK_500,
    CHANGE_RANK_1000,
    SYMBOL,
    CIK,
    DATE_ADDED,
    FOUNDED_YEAR,
    GICS_SECTOR,
    GICS_SUB_INDUSTRY
from ranked
where rn = 1
