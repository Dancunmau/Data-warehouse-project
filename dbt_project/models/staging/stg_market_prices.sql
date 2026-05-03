-- models/staging/stg_market_prices.sql
-- dbt intelligently references the raw source table using the source macro
-- This maps the transformation lineage automatically!

WITH source_data AS (
    SELECT * 
    FROM {{ source('public_raw', 'raw_market_prices') }}
)

SELECT
    -- Fix Text Casing
    INITCAP(TRIM(product)) AS product_name,
    INITCAP(TRIM(region)) AS region_name,
    
    -- Cast and clean constraints
    CAST(date AS DATE) AS date,
    CAST(price AS NUMERIC) AS avg_price,
    
    -- Operational Extraction History (Metadata hook)
    CURRENT_TIMESTAMP AS dbt_extracted_at

FROM source_data
WHERE price IS NOT NULL 
  AND price > 0
