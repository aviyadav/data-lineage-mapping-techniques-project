-- target: staging.orders

DROP TABLE IF EXISTS staging.orders;

CREATE TABLE staging.orders AS
SELECT
    order_id,
    customer_id,
    product_id,
    CAST(amount AS NUMERIC(12,2)) AS amount,
    CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
    UPPER(status) AS status
FROM raw.orders
WHERE amount >= 0
  AND status <> 'CANCELLED'
  AND customer_id IS NOT NULL;