--بسم الله
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert'
    )
}}

select *
from {{ source('fx', 'fx_rates') }}

{% if is_incremental() %}
where fetched_at > (
    select coalesce(max(fetched_at), '1900-01-01'::timestamp)
    from {{ this }}
)
{% endif %}
