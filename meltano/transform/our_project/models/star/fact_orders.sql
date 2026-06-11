WITH latest_review AS (
    SELECT
        order_id,
        review_score
    FROM {{ ref('stg_order_reviews') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id ORDER BY review_answered_at DESC
    ) = 1
),
payment_totals AS (
    SELECT
        order_id,
        SUM(payment_value) AS total_payment_value
    FROM {{ ref('stg_order_payments') }}
    GROUP BY order_id
)
SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    DATE(o.purchase_at)                       AS purchase_date,
    DATE_DIFF(
        DATE(o.delivered_customer_at),
        DATE(o.purchase_at),
        DAY
    )                                         AS delivery_days,
    DATE_DIFF(
        DATE(o.estimated_delivery_at),
        DATE(o.purchase_at),
        DAY
    )                                         AS estimated_delivery_days,
    r.review_score,
    p.total_payment_value
FROM {{ ref('stg_orders') }} o
LEFT JOIN latest_review r
    ON o.order_id = r.order_id
LEFT JOIN payment_totals p
    ON o.order_id = p.order_id
LEFT JOIN {{ ref('dq_orders') }} dq
    ON o.order_id = dq.order_id
WHERE dq.order_id IS NULL
