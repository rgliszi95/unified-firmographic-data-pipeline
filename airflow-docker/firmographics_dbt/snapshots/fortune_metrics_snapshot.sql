{% snapshot fortune_metrics_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='fortune_metrics_key',
      strategy='timestamp',
      updated_at='last_updated',
      invalidate_hard_deletes=True
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['company_name','slug']) }} as fortune_metrics_key,
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
    last_updated
from {{ ref('cr_company_complete') }}

{% endsnapshot %}