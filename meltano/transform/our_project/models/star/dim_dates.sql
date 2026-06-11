WITH date_spine AS (
    SELECT date_day
    FROM UNNEST(
        GENERATE_DATE_ARRAY('2016-01-01', '2018-12-31', INTERVAL 1 DAY)
    ) AS date_day
)
SELECT
    date_day                                   AS date_id,
    EXTRACT(YEAR FROM date_day)                AS year,
    EXTRACT(MONTH FROM date_day)               AS month,
    FORMAT_DATE('%B', date_day)                AS month_name,
    EXTRACT(QUARTER FROM date_day)             AS quarter,
    FORMAT_DATE('%A', date_day)                AS day_of_week,
    EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) AS is_weekend
FROM date_spine
