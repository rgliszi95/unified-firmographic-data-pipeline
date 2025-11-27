
select
    fortune_metrics_key,
    company_order,
    company_rank,
    slug,
    is_best_company,
    is_change_the_world,
    dropped_in_rank,
    is_future_50,
    is_global_500,
    is_profitable,
    is_newcomer,
    has_female_ceo,
    founder_is_ceo,
    is_fastest_growing,
    is_most_admired,
    change_rank_500,
    change_rank_1000,
    dbt_valid_from as valid_from
from {{ ref('fortune_metrics_snapshot') }}
where dbt_valid_to is null