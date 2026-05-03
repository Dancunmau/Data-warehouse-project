-- =============================================================
-- Advanced OLAP: CUBE & GROUPING SETS Queries
-- File: sql/olap/cube_queries.sql
-- Purpose: Demonstrate advanced multidimensional aggregation
--          on the dbt-materialized fact_agri table.
--          NOTE: fact_agri uses direct name columns (no FK IDs).
--          Run these queries in pgAdmin or any PostgreSQL client.
-- =============================================================


-- ── 1. Full Multidimensional Data Cube ───────────────────────
-- Generates all possible aggregation combinations for
-- Product, Region, and Year using CUBE.

SELECT
    COALESCE(product_name, 'GRAND TOTAL / ALL PRODUCTS') AS product,
    COALESCE(region_name,  'ALL REGIONS')                AS region,
    COALESCE(CAST(EXTRACT(YEAR FROM date) AS VARCHAR), 'ALL YEARS') AS year,
    ROUND(AVG(avg_price), 2)   AS avg_price,
    ROUND(AVG(temperature), 1) AS avg_temp,
    SUM(quantity)              AS total_supply
FROM fact_agri
GROUP BY CUBE (product_name, region_name, EXTRACT(YEAR FROM date))
ORDER BY
    GROUPING(product_name),
    GROUPING(region_name),
    GROUPING(EXTRACT(YEAR FROM date)),
    product_name,
    region_name,
    EXTRACT(YEAR FROM date);


-- ── 2. Specific Grouping Sets ────────────────────────────────
-- Target exactly the aggregations you need:
-- Grand Total, By Region, By Product, By Region & Product.

SELECT
    COALESCE(product_name, 'ALL PRODUCTS') AS product,
    COALESCE(region_name,  'ALL REGIONS')  AS region,
    ROUND(AVG(avg_price), 2)               AS avg_price,
    SUM(quantity)                          AS total_supply
FROM fact_agri
GROUP BY GROUPING SETS (
    (),                              -- Grand Total
    (region_name),                   -- By Region
    (product_name),                  -- By Product
    (region_name, product_name)      -- By Region & Product
)
ORDER BY
    GROUPING(region_name),
    GROUPING(product_name),
    region_name,
    product_name;


-- ── 3. Time Hierarchy Rollup ─────────────────────────────────
-- Aggregates hierarchically: (Year, Month) -> (Year) -> Grand Total

SELECT
    COALESCE(CAST(EXTRACT(YEAR  FROM date) AS VARCHAR), 'ALL YEARS')  AS year,
    COALESCE(CAST(EXTRACT(MONTH FROM date) AS VARCHAR), 'ALL MONTHS') AS month,
    SUM(quantity)             AS total_supply,
    ROUND(AVG(avg_price), 2) AS avg_price
FROM fact_agri
GROUP BY ROLLUP (EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date))
ORDER BY
    EXTRACT(YEAR  FROM date) NULLS LAST,
    EXTRACT(MONTH FROM date) NULLS LAST;


-- ── 4. Weather Impact Analysis ───────────────────────────────
-- Analyze how rainfall events affect average prices by region.

SELECT
    COALESCE(region_name, 'ALL REGIONS') AS region,
    CASE
        WHEN GROUPING(rainfall > 10) = 1 THEN 'ANY RAIN'
        WHEN rainfall > 10             THEN 'Heavy Rain (>10mm)'
        ELSE                                'Light/No Rain'
    END                          AS weather_condition,
    ROUND(AVG(avg_price), 2)    AS avg_price,
    SUM(quantity)               AS total_harvest
FROM fact_agri
WHERE rainfall IS NOT NULL
GROUP BY GROUPING SETS (
    (),
    (region_name),
    (rainfall > 10),
    (region_name, rainfall > 10)
)
ORDER BY region, weather_condition;
