SELECT
    product_id,
    product_category_name,
    CAST(product_name_lenght AS INT64)        AS name_length,
    CAST(product_description_lenght AS INT64) AS description_length,
    CAST(product_photos_qty AS INT64)         AS photos_qty,
    CAST(product_weight_g AS INT64)           AS weight_g,
    CAST(product_length_cm AS INT64)          AS length_cm,
    CAST(product_height_cm AS INT64)          AS height_cm,
    CAST(product_width_cm AS INT64)           AS width_cm,
    _sdc_received_at                          AS loading_date
FROM {{ source('kaggle_data', 'products') }}
