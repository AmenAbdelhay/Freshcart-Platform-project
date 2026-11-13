--بسم الله
{{
    config(
        materialized='incremental',
        unique_key='product_id',
        incremental_strategy='delete+insert'
    )
}}

select  
p.product_id ,
p.product_name ,
p.category ,
pp.unit_price_usd ,
pp.valid_from ,
p.created_at ,
pp.updated_at as last_price
from {{ source('silver', 'products') }} as p 
left join {{ source('silver', 'product_prices') }} as pp 
	on p.product_id = pp.product_id 

/*{% if is_incremental() %}              -the incremental part
where updated_at > (
    select coalesce(max(updated_at), '1900-01-01'::timestamp)
    from {{ this }}
)
{% endif %}*/
