SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    CAST(payment_value AS FLOAT64) AS payment_value,
    _sdc_received_at               AS loading_date
FROM {{ source('kaggle_data', 'order_payments') }}
