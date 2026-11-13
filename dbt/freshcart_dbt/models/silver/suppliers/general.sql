--بسم الله
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert'
    )
}}

select *
from {{ source('suppliers', 'general') }}

{% if is_incremental() %}
where _loaded_at > (
    select coalesce(max(_loaded_at), '1900-01-01'::timestamp)
    from {{ this }}
)
{% endif %}