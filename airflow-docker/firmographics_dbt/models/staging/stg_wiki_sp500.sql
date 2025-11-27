
with raw as (
    select *
    from {{ source('company_data_raw', 'wiki_sp500') }}
    {% if is_incremental() %}
        where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}
),

flattened as (
    select
        r.id as raw_id,
        r.ingested_at,
        r.source,
        f.value as data
    from raw r,
         lateral flatten(input => r.payload) f
),

cleaned as (
    select
        raw_id,
        ingested_at,
        source,
        SPLIT(data:"Security"::string ,' (')[0]::string              as company_name,
        data:"Symbol"::string                 as symbol,
        data:"CIK"::int                       as cik,
        NULLIF(data:"Date added"::string, '')::date   as date_added,
        SUBSTRING(data:"Founded"::string,1,4)::int AS founded_year,
        data:"GICS Sector"::string             as gics_sector,
        data:"GICS Sub-Industry"::string       as gics_sub_industry,
        IFF(data:"Headquarters Location"::string = 'none', NULL, SPLIT(data:"Headquarters Location"::string, ', ')[0])::string  as headquarters_location_city,
        IFF(data:"Headquarters Location"::string = 'none', NULL, SPLIT(data:"Headquarters Location"::string, ', ')[1])::string  as headquarters_location_country
    from flattened
),

deduplicated as (
    select *
    from (
        select *,
               row_number() over (partition by cik order by date_added asc) as rn
        from cleaned
    ) t
    where rn = 1
)

select 
    raw_id,
	ingested_at,
	source,
	company_name,
	symbol,
	cik,
	date_added,
	founded_year,
	gics_sector,
	gics_sub_industry,
	headquarters_location_city,
	headquarters_location_country
from deduplicated
