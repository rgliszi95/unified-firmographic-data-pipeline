{% snapshot company_location_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='location_key',
      strategy='timestamp',
      updated_at='last_updated',
      invalidate_hard_deletes=True
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['company_name','headquarters_city','headquarters_state']) }} as location_key,
    headquarters_city,
    headquarters_state,
    last_updated
from {{ ref('cr_company_complete') }}

{% endsnapshot %}