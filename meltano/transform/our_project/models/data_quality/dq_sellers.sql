WITH flagged AS (
    SELECT
        seller_id,
        (seller_id IS NULL)       AS null_seller_id,
        (zip_code_prefix IS NULL) AS null_zip_code
    FROM {{ ref('stg_sellers') }}
)
SELECT
    seller_id,
    null_seller_id,
    null_zip_code,
    (
        SELECT STRING_AGG(issue, ', ' ORDER BY issue)
        FROM UNNEST([
            IF(null_seller_id, 'null_seller_id', NULL),
            IF(null_zip_code,  'null_zip_code', NULL)
        ]) AS issue
        WHERE issue IS NOT NULL
    ) AS issues
FROM flagged
WHERE null_seller_id
   OR null_zip_code
