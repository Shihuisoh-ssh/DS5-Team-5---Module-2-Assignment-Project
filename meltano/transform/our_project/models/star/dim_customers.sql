SELECT
    c.customer_id,
    c.customer_unique_id,
    c.city,
    c.state,
    c.zip_code_prefix
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('dq_customers') }} dq
    ON c.customer_id = dq.customer_id
WHERE dq.customer_id IS NULL
