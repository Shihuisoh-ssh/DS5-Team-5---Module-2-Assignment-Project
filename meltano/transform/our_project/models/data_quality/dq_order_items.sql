WITH flagged AS (
    SELECT
        order_id,
        order_item_id,
        (order_id IS NULL)                  AS null_order_id,
        (product_id IS NULL)                AS null_product_id,
        (seller_id IS NULL)                 AS null_seller_id,
        (price IS NOT NULL AND price <= 0)  AS negative_price,
        (freight_value IS NOT NULL
            AND freight_value < 0)          AS negative_freight
    FROM {{ ref('stg_order_items') }}
)
SELECT
    order_id,
    order_item_id,
    null_order_id,
    null_product_id,
    null_seller_id,
    negative_price,
    negative_freight,
    (
        SELECT STRING_AGG(issue, ', ' ORDER BY issue)
        FROM UNNEST([
            IF(null_order_id,    'null_order_id', NULL),
            IF(null_product_id,  'null_product_id', NULL),
            IF(null_seller_id,   'null_seller_id', NULL),
            IF(negative_price,   'negative_price', NULL),
            IF(negative_freight, 'negative_freight', NULL)
        ]) AS issue
        WHERE issue IS NOT NULL
    ) AS issues
FROM flagged
WHERE null_order_id
   OR null_product_id
   OR null_seller_id
   OR negative_price
   OR negative_freight
