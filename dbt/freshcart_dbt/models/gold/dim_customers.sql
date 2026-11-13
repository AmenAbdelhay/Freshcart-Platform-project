--بسم الله
{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='delete+insert'
    )
}}

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.city,
    c.signup_date,
    c.created_at AS customer_created_at ,
    c.updated_at
from {{ source('silver', 'customers') }} as c

{% if is_incremental() %}
where updated_at > (
    select coalesce(max(updated_at), '1900-01-01'::timestamp)
    from {{ this }}
)
{% endif %}
