--بسم الله
{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='delete+insert'
    )
}}

select 
customer_id ,
trim(first_name ) as first_name ,
trim(last_name) as last_name ,
city ,
lower(trim(email)) as email,
case 
	when phone is NUll then 'n/a'
	else trim(phone)
end as phone ,
signup_date ,
created_at ,
updated_at ,
case 
	when lower(trim(email)) LIKE '%@example.com' then 0
	else 1
end as flag
from {{ source('freshcart', 'customers') }}

{% if is_incremental() %}
where updated_at > (
    select coalesce(max(updated_at), '1900-01-01'::timestamp)
    from {{ this }}
)
{% endif %}
