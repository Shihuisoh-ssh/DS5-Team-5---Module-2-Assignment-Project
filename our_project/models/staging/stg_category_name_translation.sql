SELECT
    product_category_name,
    product_category_name_english AS category_name_english,
    _sdc_received_at              AS loading_date
FROM {{ source('kaggle_data', 'category_name_translation') }}
