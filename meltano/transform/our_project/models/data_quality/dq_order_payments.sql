WITH flagged AS (
    SELECT
        order_id,
        payment_sequential,
        (order_id IS NULL)                         AS null_order_id,
        (payment_value IS NOT NULL
            AND payment_value <= 0)                AS zero_or_negative_payment_value,
        (payment_type IS NOT NULL
            AND payment_type NOT IN (
                'credit_card','boleto','voucher','debit_card'
            ))                                     AS invalid_payment_type
    FROM {{ ref('stg_order_payments') }}
)
SELECT
    order_id,
    payment_sequential,
    null_order_id,
    zero_or_negative_payment_value,
    invalid_payment_type,
    (
        SELECT STRING_AGG(issue, ', ' ORDER BY issue)
        FROM UNNEST([
            IF(null_order_id,                  'null_order_id', NULL),
            IF(zero_or_negative_payment_value, 'zero_or_negative_payment_value', NULL),
            IF(invalid_payment_type,           'invalid_payment_type', NULL)
        ]) AS issue
        WHERE issue IS NOT NULL
    ) AS issues
FROM flagged
WHERE null_order_id
   OR zero_or_negative_payment_value
   OR invalid_payment_type
