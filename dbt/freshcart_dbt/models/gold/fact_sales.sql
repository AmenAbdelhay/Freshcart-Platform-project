
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}


WITH promo (code, rate) AS (
    VALUES ('WELCOME10', 0.10), ('CAIRO20', 0.20),
           ('FRESH5',    0.05), ('WEEKEND15', 0.15)
),

-- 1) Narrow to the orders we actually need FIRST (new rows only on incremental runs)
orders_scope AS (
    SELECT
        o.order_id,
        o.placed_at,
        o.delivered_at,
        o.store_id,
        o.order_status,
        o.promo_code,
        s.currency,
        (o.delivered_at AT TIME ZONE 'Africa/Cairo')::date AS sales_date
    FROM {{ source('silver', 'orders') }} o
    JOIN {{ source('gold', 'dim_stores') }} s ON s.store_id = o.store_id
    WHERE o.order_status = 'delivered'
    {% if is_incremental() %}
      AND o.delivered_at > (
            SELECT COALESCE(MAX(delivered_at), '1900-01-01'::timestamptz)
            FROM {{ this }}
      )
    {% endif %}
),

-- 2) Aggregate items/payments only for those orders (no full-table scans)
items AS (
    SELECT oi.order_id, SUM(oi.line_amount) AS gross
    FROM {{ source('silver', 'order_items') }} oi
    WHERE oi.order_id IN (SELECT order_id FROM orders_scope)
    GROUP BY oi.order_id
),

pay AS (
    SELECT
        p.order_id,
        SUM(p.amount) FILTER (WHERE LOWER(TRIM(p.payment_status)) = 'paid')     AS paid,
        SUM(p.amount) FILTER (WHERE LOWER(TRIM(p.payment_status)) = 'refunded') AS refunded
    FROM {{ source('silver', 'payments') }} p
    WHERE p.order_id IN (SELECT order_id FROM orders_scope)
    GROUP BY p.order_id
),

-- 3) Look up FX once per (currency, date) instead of once per order
fx_needed AS (
    SELECT DISTINCT currency, sales_date
    FROM orders_scope
    WHERE currency <> 'USD'
),

fx_lookup AS (
    SELECT
        n.currency,
        n.sales_date,
        (
            SELECT (f.rates ->> n.currency)::numeric
            FROM {{ source('silver', 'fx_rate') }} f
            WHERE f.base_currency = 'USD'
              AND f.rate_date <= n.sales_date
              AND f.rates ->> n.currency IS NOT NULL
            ORDER BY f.rate_date DESC
            LIMIT 1
        ) AS rate
    FROM fx_needed n
),

base AS (
    SELECT
        os.order_id,
        os.placed_at,
        os.delivered_at,
        os.store_id,
        os.order_status,
        os.currency,
        CASE WHEN os.currency = 'USD' THEN 1::numeric ELSE fx.rate END AS fx_rate,

        COALESCE(i.gross, 0)                           AS gross_revenue_local,
        COALESCE(i.gross, 0) * COALESCE(pr.rate, 0)    AS discount_amount_local,
        COALESCE(p.refunded, 0)                        AS refunded_amount_local,
        COALESCE(p.paid, 0) - COALESCE(p.refunded, 0)  AS cash_collected_local
    FROM orders_scope os
    LEFT JOIN items     i  ON i.order_id = os.order_id
    LEFT JOIN pay       p  ON p.order_id = os.order_id
    LEFT JOIN promo     pr ON pr.code = UPPER(TRIM(os.promo_code))
    LEFT JOIN fx_lookup fx ON fx.currency = os.currency
                          AND fx.sales_date = os.sales_date
)

SELECT
    b.order_id,
    b.placed_at,
    b.delivered_at,
    b.store_id,
    b.currency,
    b.order_status,
    b.fx_rate,

    -- Local
    b.gross_revenue_local,
    b.discount_amount_local,
    b.refunded_amount_local,
    b.gross_revenue_local - b.discount_amount_local - b.refunded_amount_local AS net_revenue_local,
    b.cash_collected_local,

    -- USD
    b.gross_revenue_local   / b.fx_rate AS gross_revenue_usd,
    b.discount_amount_local / b.fx_rate AS discount_amount_usd,
    b.refunded_amount_local / b.fx_rate AS refunded_amount_usd,
    (b.gross_revenue_local - b.discount_amount_local - b.refunded_amount_local)
                            / b.fx_rate AS net_revenue_usd,
    b.cash_collected_local  / b.fx_rate AS cash_collected_usd
FROM base b

{% if is_incremental() %}
where delivered_at > (
    select coalesce(max(delivered_at), '1900-01-01'::timestamp)
    from {{ this }}
)

{% endif %}
