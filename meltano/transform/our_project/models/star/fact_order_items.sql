SELECT
    i.order_id,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    o.customer_id,
    DATE(o.purchase_at)       AS purchase_date,
    i.price,
    i.freight_value,
    i.price + i.freight_value AS total_item_cost
FROM {{ ref('stg_order_items') }} i
INNER JOIN {{ ref('stg_orders') }} o
    ON i.order_id = o.order_id
LEFT JOIN {{ ref('dq_order_items') }} dq
    ON i.order_id = dq.order_id
    AND i.order_item_id = dq.order_item_id
WHERE dq.order_id IS NULL
