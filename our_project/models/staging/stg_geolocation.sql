SELECT
    CAST(geolocation_zip_code_prefix AS STRING) AS zip_code_prefix,
    CAST(geolocation_lat AS FLOAT64)            AS lat,
    CAST(geolocation_lng AS FLOAT64)            AS lng,
    geolocation_city                            AS city,
    geolocation_state                           AS state,
    _sdc_received_at                            AS loading_date
FROM {{ source('kaggle_data', 'geolocation') }}
