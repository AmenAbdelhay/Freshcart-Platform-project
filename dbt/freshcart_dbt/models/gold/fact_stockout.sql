

{{
    config(
        materialized='incremental',
        unique_key='product_id',
        incremental_strategy='delete+insert'
    )
}}

with suppliers_ranked as (
    select
        ds.store_id,
        ds.product_id,
        ds.supplier,
        ds.stock_source,
        ds.quantity_on_hand,
        ds.minimum_quantity,
        ds.quantity_difference,
        ds.unit_cost,
        ds.stock_date,
        ds.stock_status,
        ds.needs_reorder,
        ds.store_updated_at,
        row_number() over (
            partition by ds.store_id, ds.product_id
            order by ds.stock_date desc, ds.store_updated_at desc
        ) as rn
    from silver_gold.dim_suppliers ds
),

current_stock as (
    select * from suppliers_ranked where rn = 1
),

products_latest as (
    select
        product_id,
        product_name,
        category,
        unit_price_usd,
        row_number() over (
            partition by product_id
            order by valid_from desc
        ) as rn
    from silver_gold.dim_products
),

stores_latest as (
    select
        store_id,
        store_name,
        city,
        country,
        row_number() over (
            partition by store_id
            order by updated_at desc
        ) as rn
    from silver_gold.dim_stores
)

select
    cs.store_id,
    s.store_name,
    s.city,
    s.country,
    cs.supplier,
    cs.stock_source,
    cs.product_id,
    p.product_name,
    p.category,
    p.unit_price_usd,
    cs.unit_cost,
    cs.quantity_on_hand,
    cs.minimum_quantity,
    cs.quantity_difference,
    greatest(cs.minimum_quantity - cs.quantity_on_hand, 0) as shortage_qty,
    cs.quantity_on_hand * cs.unit_cost                      as stock_value_cost,
    cs.stock_status,
    cs.needs_reorder,
    cs.stock_date,
    cs.store_updated_at
from current_stock cs
left join products_latest p
    on p.product_id = cs.product_id and p.rn = 1
left join stores_latest s
    on s.store_id = cs.store_id and s.rn = 1

/*{% if is_incremental() %}   
where updated_at > (
    select coalesce(max(updated_at), '1900-01-01'::timestamp)
    from {{ this }}
)
{% endif %}*/