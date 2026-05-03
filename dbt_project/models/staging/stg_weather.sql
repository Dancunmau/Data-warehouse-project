WITH source_data AS (
    SELECT * 
    FROM {{ source('public_raw', 'raw_weather') }}
)

SELECT
    INITCAP(TRIM(region)) AS region_name,
    CAST(date AS DATE) AS date,
    CAST(temperature AS NUMERIC) AS temperature,
    CAST(rainfall AS NUMERIC) AS rainfall,
    CURRENT_TIMESTAMP AS dbt_extracted_at
FROM source_data
WHERE temperature IS NOT NULL
