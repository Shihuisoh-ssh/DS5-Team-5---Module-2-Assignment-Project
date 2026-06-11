SELECT
    s.seller_id,
    s.city,
    s.state,
    s.zip_code_prefix
FROM {{ ref('stg_sellers') }} s
LEFT JOIN {{ ref('dq_sellers') }} dq
    ON s.seller_id = dq.seller_id
WHERE dq.seller_id IS NULL
