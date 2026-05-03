-- models/warehouse/fact_agri.sql
-- The core Star Schema Fact table.
-- dbt will compile this into `CREATE TABLE fact_agri AS SELECT...` behind the scenes!

WITH market AS (
    SELECT * FROM {{ ref('stg_market_prices') }}
),
weather AS (
    SELECT * FROM {{ ref('stg_weather') }}
),
production AS (
    SELECT * FROM {{ ref('stg_production') }}
)

SELECT
    -- Use md5() macro to generate a unique surrogate key over the business grain
    md5(CAST(m.date AS VARCHAR) || m.product_name || m.region_name) AS fact_id,
    
    m.date,
    m.product_name,
    m.region_name,
    
    -- Measures
    m.avg_price,
    COALESCE(p.quantity, 0) AS quantity,
    w.temperature,
    w.rainfall,
    
    CURRENT_TIMESTAMP AS dbt_loaded_at

FROM market m
LEFT JOIN weather w 
    ON m.date = w.date AND m.region_name = w.region_name
LEFT JOIN production p 
    ON m.date = p.date AND m.product_name = p.product_name AND m.region_name = p.region_name
