--بسم الله
{{
    config(
        materialized='incremental',
        unique_key='product_id',
        incremental_strategy='delete+insert'
    )
}}

select 
s.store_id ,
s.store_name ,
ds.needs_reorder ,
ds.stock_status ,
ds.supplier ,
ds.city ,
dp.product_id ,
dp.product_name ,
dp.category ,
ds.store_updated_at 
from {{ source('gold', 'dim_suppliers') }} ds 
left join {{ source('gold', 'dim_products') }} dp 
on dp.product_id = ds.product_id 
left join {{ source('gold', 'dim_stores') }} s
on s.store_id = ds.store_id 

{% if is_incremental() %}
where store_updated_at > (
    select coalesce(max(store_updated_at), '1900-01-01'::timestamp)
    from {{ this }}
)
{% endif %}
