-- =============================================================
-- OLAP Query Patterns
-- File: sql/olap/olap_queries.sql
-- Purpose: Multidimensional analysis queries demonstrating
--          slice, dice, rollup, and cube patterns on fact_agri.
-- =============================================================

-- ── 1. ROLLUP – Price totals across product → region → all ──

SELECT
    COALESCE(p.product_name, '** ALL PRODUCTS **') AS product,
    COALESCE(r.region_name,  '** ALL REGIONS **')  AS region,
    ROUND(AVG(f.price), 2)                          AS avg_price,
    COUNT(*)                                        AS records
FROM fact_agri f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_region  r ON f.region_id  = r.region_id
GROUP BY ROLLUP(p.product_name, r.region_name)
ORDER BY product, region;


-- ── 2. CUBE – Price by product, region, and year ─────────────

SELECT
    COALESCE(p.product_name, '** ALL **') AS product,
    COALESCE(r.region_name,  '** ALL **') AS region,
    COALESCE(CAST(d.year AS VARCHAR), '** ALL **') AS year,
    ROUND(AVG(f.price), 2) AS avg_price
FROM fact_agri f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_region  r ON f.region_id  = r.region_id
JOIN dim_date    d ON f.date_id    = d.date_id
GROUP BY CUBE(p.product_name, r.region_name, d.year)
ORDER BY product, region, year;


-- ── 3. SLICE – Single product: Maize only ────────────────────

SELECT
    d.year,
    d.month,
    r.region_name,
    ROUND(AVG(f.price),    2) AS avg_price,
    SUM(f.quantity)           AS total_qty,
    ROUND(AVG(f.rainfall), 1) AS avg_rainfall
FROM fact_agri f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_region  r ON f.region_id  = r.region_id
JOIN dim_date    d ON f.date_id    = d.date_id
WHERE p.product_name = 'Maize'
GROUP BY d.year, d.month, r.region_name
ORDER BY d.year, d.month, r.region_name;


-- ── 4. DICE – Maize & Beans in Nairobi & Eldoret, 2023 ───────

SELECT
    d.date,
    p.product_name,
    r.region_name,
    f.price,
    f.quantity,
    f.temperature,
    f.rainfall
FROM fact_agri f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_region  r ON f.region_id  = r.region_id
JOIN dim_date    d ON f.date_id    = d.date_id
WHERE p.product_name IN ('Maize', 'Beans')
  AND r.region_name  IN ('Nairobi', 'Eldoret')
  AND d.year = 2023
ORDER BY d.date, p.product_name, r.region_name;


-- ── 5. DRILL DOWN – Annual → Quarterly → Monthly price ───────

-- Annual
SELECT d.year, ROUND(AVG(f.price), 2) AS avg_price
FROM fact_agri f JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year ORDER BY d.year;

-- Quarterly
SELECT d.year, d.quarter, ROUND(AVG(f.price), 2) AS avg_price
FROM fact_agri f JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.quarter ORDER BY d.year, d.quarter;

-- Monthly
SELECT d.year, d.month, ROUND(AVG(f.price), 2) AS avg_price
FROM fact_agri f JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month ORDER BY d.year, d.month;


-- ── 6. WEATHER IMPACT – Rainfall vs. price correlation ───────

SELECT
    p.product_name,
    r.region_name,
    CASE
        WHEN f.rainfall = 0            THEN 'Dry'
        WHEN f.rainfall BETWEEN 1 AND 15 THEN 'Light Rain'
        WHEN f.rainfall BETWEEN 16 AND 30 THEN 'Moderate Rain'
        ELSE                                'Heavy Rain'
    END                          AS rain_category,
    COUNT(*)                     AS days,
    ROUND(AVG(f.price),    2)    AS avg_price,
    ROUND(AVG(f.quantity), 0)    AS avg_quantity
FROM fact_agri f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_region  r ON f.region_id  = r.region_id
WHERE f.rainfall IS NOT NULL AND f.quantity IS NOT NULL
GROUP BY p.product_name, r.region_name, rain_category
ORDER BY p.product_name, r.region_name, rain_category;


-- ── 7. RANKING – Top producing region per product per year ───

SELECT
    year,
    product_name,
    region_name,
    total_qty,
    supply_rank
FROM (
    SELECT
        d.year,
        p.product_name,
        r.region_name,
        SUM(f.quantity)   AS total_qty,
        RANK() OVER (
            PARTITION BY d.year, p.product_name
            ORDER BY SUM(f.quantity) DESC
        ) AS supply_rank
    FROM fact_agri f
    JOIN dim_product p ON f.product_id = p.product_id
    JOIN dim_region  r ON f.region_id  = r.region_id
    JOIN dim_date    d ON f.date_id    = d.date_id
    WHERE f.quantity IS NOT NULL
    GROUP BY d.year, p.product_name, r.region_name
) ranked
WHERE supply_rank = 1
ORDER BY year, product_name;


-- ── 8. PERIOD-OVER-PERIOD – YoY price change per product ─────

WITH monthly_avg AS (
    SELECT
        d.year,
        d.month,
        p.product_name,
        ROUND(AVG(f.price), 2) AS avg_price
    FROM fact_agri f
    JOIN dim_product p ON f.product_id = p.product_id
    JOIN dim_date    d ON f.date_id    = d.date_id
    GROUP BY d.year, d.month, p.product_name
)
SELECT
    curr.year,
    curr.month,
    curr.product_name,
    curr.avg_price                                         AS current_price,
    prev.avg_price                                         AS prior_year_price,
    ROUND(curr.avg_price - prev.avg_price, 2)              AS price_change,
    ROUND(100.0 * (curr.avg_price - prev.avg_price)
          / NULLIF(prev.avg_price, 0), 1)                  AS pct_change
FROM monthly_avg curr
LEFT JOIN monthly_avg prev
       ON curr.product_name = prev.product_name
      AND curr.month        = prev.month
      AND curr.year         = prev.year + 1
ORDER BY curr.product_name, curr.year, curr.month;
