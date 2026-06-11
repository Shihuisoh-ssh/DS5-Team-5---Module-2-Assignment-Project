WITH flagged AS (
    SELECT
        zip_code_prefix,
        lat,
        lng,
        (lat IS NULL OR lng IS NULL)                    AS null_lat_lng,
        (lat IS NOT NULL AND lng IS NOT NULL
            AND (lat NOT BETWEEN -33.75 AND 5.27
                OR lng NOT BETWEEN -73.99 AND -34.79))  AS out_of_range_coordinates
    FROM {{ ref('stg_geolocation') }}
)
SELECT
    zip_code_prefix,
    lat,
    lng,
    null_lat_lng,
    out_of_range_coordinates,
    (
        SELECT STRING_AGG(issue, ', ' ORDER BY issue)
        FROM UNNEST([
            IF(null_lat_lng,             'null_lat_lng', NULL),
            IF(out_of_range_coordinates, 'out_of_range_coordinates', NULL)
        ]) AS issue
        WHERE issue IS NOT NULL
    ) AS issues
FROM flagged
WHERE null_lat_lng
   OR out_of_range_coordinates
