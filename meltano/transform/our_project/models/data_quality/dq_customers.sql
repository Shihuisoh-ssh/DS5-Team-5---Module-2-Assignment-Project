WITH flagged AS (
    SELECT
        customer_id,
        (customer_id IS NULL)        AS null_customer_id,
        (customer_unique_id IS NULL) AS null_customer_unique_id,
        (zip_code_prefix IS NULL)    AS null_zip_code
    FROM {{ ref('stg_customers') }}
)
SELECT
    customer_id,
    null_customer_id,
    null_customer_unique_id,
    null_zip_code,
    (
        SELECT STRING_AGG(issue, ', ' ORDER BY issue)
        FROM UNNEST([
            IF(null_customer_id,        'null_customer_id', NULL),
            IF(null_customer_unique_id, 'null_customer_unique_id', NULL),
            IF(null_zip_code,           'null_zip_code', NULL)
        ]) AS issue
        WHERE issue IS NOT NULL
    ) AS issues
FROM flagged
WHERE null_customer_id
   OR null_customer_unique_id
   OR null_zip_code
