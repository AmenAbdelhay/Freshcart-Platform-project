--بسم الله
{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        incremental_strategy='delete+insert'
    )
}}

select 
* 
from {{ source('freshcart' , 'order_items') }}

{% if is_incremental() %}
where updated_at > (
    select coalesce(max(updated_at), '1900-01-01'::timestamp)
    from {{ this }}
)
{% endif %}



