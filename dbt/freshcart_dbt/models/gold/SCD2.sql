--بسم الله

{{
    config(
        materialized='view'
    )
}}


with dedup as (
    select distinct
        product_id,
        unit_price_usd,
        valid_from,
        created_at,
        last_price
    from silver_gold.dim_products
)

select
    product_id,
    unit_price_usd,
    valid_from,
    created_at,
    last_price,
    row_number() over (
        partition by product_id
        order by valid_from desc
    ) = 1 as is_current
from dedup