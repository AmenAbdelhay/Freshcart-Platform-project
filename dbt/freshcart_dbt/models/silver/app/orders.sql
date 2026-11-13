
-- بسم الله
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}

select
    customer_id,
    order_id,
    store_id,
    order_status,
    placed_at,
    delivered_at,
    created_at,
    updated_at,
    case
        when promo_code is null then 'n/a'
        else promo_code
    end as promo_code

from {{ source('freshcart', 'orders') }}

{% if is_incremental() %}
where updated_at >= (
    select coalesce(
        max(updated_at) - interval '2 days',
        '1900-01-01'::timestamp
    )
    from {{ this }}
)
{% endif %}
