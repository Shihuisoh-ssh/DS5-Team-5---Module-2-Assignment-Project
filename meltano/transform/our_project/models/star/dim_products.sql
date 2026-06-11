SELECT
    p.product_id,
    p.product_category_name                      AS category_name,
    COALESCE(t.category_name_english, 'unknown') AS category_name_english,
    p.name_length,
    p.description_length,
    p.photos_qty,
    p.weight_g
FROM {{ ref('stg_products') }} p
LEFT JOIN {{ ref('stg_category_name_translation') }} t
    ON p.product_category_name = t.product_category_name
LEFT JOIN {{ ref('dq_products') }} dq
    ON p.product_id = dq.product_id
WHERE dq.product_id IS NULL
