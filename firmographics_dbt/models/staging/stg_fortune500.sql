
with raw as (
    select *
    from {{ source('company_data_raw', 'fortune_500') }}
    {% if is_incremental() %}
        where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}
),

flattened as (
    select
        r.id as raw_id,
        r.ingested_at,
        r.source,
        f.value:data as data,

        f.value:name::string              as company_name,
        f.value:order::int                as company_order,
        f.value:rank::int                 as company_rank,
        f.value:slug::string              as slug

    from raw r,
         lateral flatten(input => r.payload:items) f
),

cleaned as (
    select
        raw_id,
        ingested_at,
        source,
        company_name,
        company_order,
        company_rank,
        slug,

        {{ parse_money_to_float('data:"Assets ($M)"') }} as assets_m,
        {{ parse_money_to_float('data:"Revenues ($M)"') }} as revenues_m,
        {{ parse_money_to_float('data:"Profits ($M)"') }} as profits_m,
        {{ parse_money_to_float('data:"Market Value ($M)"') }} as market_value_m,

        NULLIF(replace(data:"Employees"::string, ',', ''), '')::int as employees,
        COALESCE(NULLIF(replace(data:"Revenue Percent Change"::string, '%', ''), '')::float, 0) as revenue_pct_change,
        COALESCE(NULLIF(replace(data:"Profits Percent Change"::string, '%', ''), '')::float, 0) as profit_pct_change,

        data:"Headquarters City"::string                                    as headquarters_city,
        data:"State"::string                                                as headquarters_state,
        data:"Industry"::string                                             as industry,
        data:"Sector"::string                                               as sector,

        iff(data:"Best Companies" = 'yes', true, false)                     as is_best_company,
        iff(data:"Change the World" = 'yes', true, false)                   as is_change_the_world,
        iff(data:"Dropped in Rank" = 'yes', true, false)                    as dropped_in_rank,
        iff(data:"Future 50" = 'yes', true, false)                          as is_future_50,
        iff(data:"Global 500" = 'yes', true, false)                         as is_global_500,
        iff(data:"Profitable" = 'yes', true, false)                         as is_profitable,
        iff(data:"Newcomer to the Fortune 500" = 'yes', true, false)        as is_newcomer,
        iff(data:"Female CEO" = 'yes', true, false)                         as has_female_ceo,
        iff(data:"Founder is CEO" = 'yes', true, false)                     as founder_is_ceo,
        iff(data:"Fastest Growing Companies" = 'yes', true, false)          as is_fastest_growing,
        iff(data:"World's Most Admired Companies" = 'yes', true, false)     as is_most_admired,

        COALESCE(NULLIF(data:"Change in Rank (500 only)"::string, '')::float, 0)         as change_rank_500,
        COALESCE(NULLIF(data:"Change in Rank (Full 1000)"::string, '')::float, 0)       as change_rank_1000
    from flattened
)

select * from cleaned
