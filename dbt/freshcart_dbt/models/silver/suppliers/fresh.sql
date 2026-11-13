--بسم الله
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert'
    )
}}

SELECT 
    _source_file,
	product_id,
	"QtyOnHands",
    supplier,
    store_id,
    reorder_level,
	_file_hash,
	_stock_date,
    _loaded_at
from {{ source('suppliers', 'fresh') }}

{% if is_incremental() %}
where _loaded_at > (
    select coalesce(max(_loaded_at), '1900-01-01'::timestamp)
    from {{ this }}
)
{% endif %}



