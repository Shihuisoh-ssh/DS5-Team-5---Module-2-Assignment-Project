WITH flagged AS (
    SELECT
        order_id,
        (customer_id IS NULL)                                          AS null_customer_id,
        (order_status IS NULL)                                         AS null_order_status,
        (order_status IS NOT NULL AND order_status NOT IN (
            'delivered','shipped','canceled','unavailable',
            'processing','created','invoiced','approved'
        ))                                                             AS invalid_order_status,
        (delivered_customer_at IS NOT NULL
            AND purchase_at IS NOT NULL
            AND delivered_customer_at < purchase_at)                   AS delivered_before_purchased
    FROM {{ ref('stg_orders') }}
)
SELECT
    order_id,
    null_customer_id,
    null_order_status,
    invalid_order_status,
    delivered_before_purchased,
    (
        SELECT STRING_AGG(issue, ', ' ORDER BY issue)
        FROM UNNEST([
            IF(null_customer_id,           'null_customer_id', NULL),
            IF(null_order_status,          'null_order_status', NULL),
            IF(invalid_order_status,       'invalid_order_status', NULL),
            IF(delivered_before_purchased, 'delivered_before_purchased', NULL)
        ]) AS issue
        WHERE issue IS NOT NULL
    ) AS issues
FROM flagged
WHERE null_customer_id
   OR null_order_status
   OR invalid_order_status
   OR delivered_before_purchased
