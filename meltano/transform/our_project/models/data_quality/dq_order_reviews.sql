WITH flagged AS (
    SELECT
        review_id,
        order_id,
        (review_id IS NULL)                              AS null_review_id,
        (order_id IS NULL)                               AS null_order_id,
        (review_score IS NOT NULL
            AND review_score NOT BETWEEN 1 AND 5)        AS invalid_review_score
    FROM {{ ref('stg_order_reviews') }}
)
SELECT
    review_id,
    order_id,
    null_review_id,
    null_order_id,
    invalid_review_score,
    (
        SELECT STRING_AGG(issue, ', ' ORDER BY issue)
        FROM UNNEST([
            IF(null_review_id,       'null_review_id', NULL),
            IF(null_order_id,        'null_order_id', NULL),
            IF(invalid_review_score, 'invalid_review_score', NULL)
        ]) AS issue
        WHERE issue IS NOT NULL
    ) AS issues
FROM flagged
WHERE null_review_id
   OR null_order_id
   OR invalid_review_score
