-- target: marts.revenue_summary

DROP TABLE IF EXISTS marts.revenue_summary;

CREATE TABLE marts.revenue_summary AS
SELECT
    o.customer_id,
    c.region,
    SUM(o.amount) AS total_revenue,
    COUNT(*) AS order_count,
    MAX(o.order_timestamp) AS last_order_at
FROM staging.orders o
JOIN raw.customers c
  ON o.customer_id = c.customer_id
GROUP BY
    o.customer_id,
    c.region;