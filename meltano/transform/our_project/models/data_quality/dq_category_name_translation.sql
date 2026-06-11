WITH flagged AS (
    SELECT
        product_category_name,
        (product_category_name IS NULL) AS null_category_name,
        (category_name_english IS NULL) AS null_english_name
    FROM {{ ref('stg_category_name_translation') }}
)
SELECT
    product_category_name,
    null_category_name,
    null_english_name,
    (
        SELECT STRING_AGG(issue, ', ' ORDER BY issue)
        FROM UNNEST([
            IF(null_category_name, 'null_category_name', NULL),
            IF(null_english_name,  'null_english_name', NULL)
        ]) AS issue
        WHERE issue IS NOT NULL
    ) AS issues
FROM flagged
WHERE null_category_name
   OR null_english_name
