--بسم الله
{{
    config(
        materialized='view'
    )
}}

SELECT
    store_id,
    currency,
    (delivered_at AT TIME ZONE 'Africa/Cairo')::date AS sales_date,

    COUNT(DISTINCT order_id)                         AS order_numbers,

    -- Local
    ROUND(SUM(net_revenue_local), 2)                 AS total_net_revenue_local,
    ROUND(SUM(cash_collected_local), 2)              AS total_cash_collected_local,
    ROUND(AVG(net_revenue_local), 2)                 AS basket_avg_local,

    -- USD
    ROUND(SUM(net_revenue_usd), 2)                   AS total_net_revenue_usd,
    ROUND(SUM(cash_collected_usd), 2)                AS total_cash_collected_usd,
    ROUND(AVG(net_revenue_usd), 2)                   AS basket_avg_usd,

    -- Delivery time (minutes)
    ROUND(
        (AVG(EXTRACT(EPOCH FROM (delivered_at -placed_at)) / 60)
            FILTER (WHERE placed_at IS NOT NULL AND delivered_at >= placed_at)
        )::numeric, 1
    )                                                AS avg_delivery_minutes

FROM silver_gold.fact_sales
WHERE order_status = 'delivered'
  AND delivered_at IS NOT NULL
GROUP BY
    store_id,
    currency,
    (delivered_at AT TIME ZONE 'Africa/Cairo')::date
ORDER BY
    sales_date,
    store_id