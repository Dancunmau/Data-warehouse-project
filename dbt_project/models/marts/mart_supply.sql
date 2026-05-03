-- models/marts/mart_supply.sql
-- Monthly aggregations for supply/production analysis.

WITH fact AS (
    SELECT * FROM {{ ref('fact_agri') }}
)

SELECT
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    product_name,
    region_name,
    SUM(quantity) AS total_quantity,
    ROUND(AVG(temperature), 1) AS avg_temp,
    ROUND(AVG(rainfall), 1) AS avg_rainfall
FROM fact
GROUP BY 1, 2, 3, 4
