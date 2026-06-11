SELECT
    review_id,
    order_id,
    CAST(review_score AS INT64)        AS review_score,
    NULLIF(review_comment_title, '')   AS review_comment_title,
    NULLIF(review_comment_message, '') AS review_comment_message,
    TIMESTAMP(review_creation_date)    AS review_created_at,
    TIMESTAMP(review_answer_timestamp) AS review_answered_at,
    _sdc_received_at                   AS loading_date
FROM {{ source('kaggle_data', 'order_reviews') }}
