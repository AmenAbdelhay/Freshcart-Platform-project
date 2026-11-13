
--بسم الله
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert'
    )
}}

WITH fresh_stock AS (
    SELECT
        f.product_id,
        f.store_id,
        f.supplier,
        f."QtyOnHands"::numeric AS quantity_on_hand,
        f.reorder_level::numeric AS minimum_quantity,
        NULL::numeric AS unit_cost,
        f._stock_date::text AS stock_date,
        'fresh' AS stock_source
    FROM silver_silver.fresh AS f
),

general_stock AS (
    SELECT
        g."Product"::integer AS product_id,
        g."Store"::integer AS store_id,
        g."Supplier" AS supplier,
        NULLIF(TRIM(g."QtyOnHands"::text), 'N/A')::numeric
    		AS quantity_on_hand,
        g."MinQty"::numeric AS minimum_quantity,
        g."UnitCost"::numeric AS unit_cost,
        NULLIF(TRIM(g."Date"), '') AS stock_date,
        'general' AS stock_source
    FROM silver_silver."general" AS g
),

all_stock AS (
    SELECT * FROM fresh_stock

    UNION ALL

    SELECT * FROM general_stock
)

SELECT
    i.product_id,
    i.store_id,
    s.store_name,
    s.city,
    s.country,
    i.supplier,
    i.stock_source,
    i.quantity_on_hand,
    i.minimum_quantity,
    i.unit_cost,
    i.stock_date,

    i.quantity_on_hand - i.minimum_quantity
        AS quantity_difference,

    CASE
        WHEN i.quantity_on_hand IS NULL
          OR i.minimum_quantity IS NULL
            THEN 'unknown'
        WHEN i.quantity_on_hand <= 0
            THEN 'out_of_stock'
        WHEN i.quantity_on_hand <= i.minimum_quantity
            THEN 'low_stock'
        ELSE 'in_stock'
    END AS stock_status,

    CASE
        WHEN i.quantity_on_hand IS NULL
          OR i.minimum_quantity IS NULL
            THEN NULL
        WHEN i.quantity_on_hand <= i.minimum_quantity
            THEN TRUE
        ELSE FALSE
    END AS needs_reorder,

    s.updated_at AS store_updated_at

FROM all_stock AS i

LEFT JOIN silver_silver.stores AS s
    ON i.store_id = s.store_id

/*{% if is_incremental() %}             -the incremental part
where updated_at > (
    select coalesce(max(updated_at), '1900-01-01'::timestamp)
    from {{ this }}
)
{% endif %}*/