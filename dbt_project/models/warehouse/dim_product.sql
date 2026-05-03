-- models/warehouse/dim_product.sql
-- In dbt, dimensions can be dynamically derived directly from the staging data
-- ensuring no duplicate key errors ever happen!

WITH stg_prices AS (
    SELECT product_name FROM {{ ref('stg_market_prices') }}
),
stg_production AS (
    SELECT product_name FROM {{ ref('stg_production') }}
)

SELECT DISTINCT product_name
FROM (
    SELECT product_name FROM stg_prices
    UNION ALL
    SELECT product_name FROM stg_production
) AS combined_products
WHERE product_name IS NOT NULL
