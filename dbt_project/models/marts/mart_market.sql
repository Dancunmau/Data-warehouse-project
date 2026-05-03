-- models/marts/mart_market.sql
-- Monthly aggregations for price analysis dashboards.

WITH fact AS (
    SELECT * FROM {{ ref('fact_agri') }}
)

SELECT
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    product_name,
    region_name,
    ROUND(AVG(avg_price), 2) AS avg_price,
    MAX(avg_price) AS max_price,
    MIN(avg_price) AS min_price,
    COUNT(*) AS record_count
FROM fact
GROUP BY 1, 2, 3, 4
