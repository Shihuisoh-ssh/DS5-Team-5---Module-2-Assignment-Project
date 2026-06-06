SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    TIMESTAMP(shipping_limit_date) AS shipping_limit_at,
    CAST(price AS FLOAT64)         AS price,
    CAST(freight_value AS FLOAT64) AS freight_value,
    _sdc_received_at               AS loading_date
FROM {{ source('kaggle_data', 'order_items') }}
