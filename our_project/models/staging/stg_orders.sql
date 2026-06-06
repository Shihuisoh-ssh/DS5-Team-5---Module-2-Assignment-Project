SELECT
    order_id,
    customer_id,
    order_status,
    TIMESTAMP(order_purchase_timestamp)      AS purchase_at,
    TIMESTAMP(order_approved_at)             AS approved_at,
    TIMESTAMP(order_delivered_carrier_date)  AS delivered_carrier_at,
    TIMESTAMP(order_delivered_customer_date) AS delivered_customer_at,
    TIMESTAMP(order_estimated_delivery_date) AS estimated_delivery_at,
    _sdc_received_at                         AS loading_date
FROM {{ source('kaggle_data', 'orders') }}
