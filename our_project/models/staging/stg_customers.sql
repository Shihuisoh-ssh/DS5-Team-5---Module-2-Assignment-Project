SELECT
    customer_id,
    customer_unique_id,
    CAST(customer_zip_code_prefix AS STRING) AS zip_code_prefix,
    customer_city                            AS city,
    customer_state                           AS state,
    _sdc_received_at                         AS loading_date
FROM {{ source('kaggle_data', 'customers') }}
