WITH source_data AS (
    SELECT * 
    FROM {{ source('public_raw', 'raw_production') }}
)

SELECT
    INITCAP(TRIM(product)) AS product_name,
    INITCAP(TRIM(region)) AS region_name,
    CAST(date AS DATE) AS date,
    CAST(quantity AS INTEGER) AS quantity,
    CURRENT_TIMESTAMP AS dbt_extracted_at
FROM source_data
