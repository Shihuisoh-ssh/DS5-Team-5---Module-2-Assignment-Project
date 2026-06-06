SELECT
    seller_id,
    CAST(seller_zip_code_prefix AS STRING) AS zip_code_prefix,
    seller_city                            AS city,
    seller_state                           AS state,
    _sdc_received_at                       AS loading_date
FROM {{ source('kaggle_data', 'sellers') }}
