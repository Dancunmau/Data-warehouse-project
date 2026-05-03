# Kozar's 7 Deadly Sins of Data Warehousing: Architecture Review

In 1997, Kozar outlined "The Seven Deadly Sins of Data Warehousing" — classic mistakes that cause multi-million dollar data projects to fail. Our AgriSupply Warehouse was specifically engineered to avoid these traps. Here is our report card!

---

## 1. "If you build it, they will come"
**The Sin:** IT builds a giant warehouse full of raw data and expects business analysts to figure out how to query it. No one uses it because it is too complex.
**Our Solution (PASSED):** We did not just build a `fact_agri` table and stop. We specifically built **Data Marts** (`mart_market.csv`, `mart_supply.csv`). These marts are pre-aggregated storefronts specifically tailored to a single business team (like the supply chain team or pricing team). We built exactly what they needed to see.

## 2. "Data warehouse architecture equals database architecture"
**The Sin:** Treating the warehouse like a normal application database (OLTP) instead of an analytical engine (OLAP).
**Our Solution (PASSED):** We built a highly denormalized **Star Schema**. In normal databases, we would have 30 interlinked tables. In our warehouse, we have exactly 1 fact table surrounded by 3 dimensions, maximizing read performance for heavy analytics.

## 3. "The data is clean"
**The Sin:** Assuming the source data (weather apps, market surveys) is perfect. 
**Our Solution (PASSED):** We rejected this assumption entirely. We built a rigorous **Staging & Validation Layer** (`sql/staging/validate_*.sql` and `ingest_raw.py`). We check for nulls, validate boundaries (weather cannot be 100°C), and standardize product names *before* the data ever reaches the warehouse.

## 4. "Data warehouse equals the data warehouse project"
**The Sin:** Viewing the warehouse as a one-time project instead of a living, breathing program that requires maintenance and evolution.
**Our Solution (PASSED):** By implementing `run_pipeline.py` and creating `etl_logger.py` (Operational Metadata), we treated this as a recurring software engineering pipeline that logs its own health over time, not a one-dump script.

## 5. "Managers will naturally use the tools"
**The Sin:** Handing business managers raw SQL access and assuming they will write queries.
**Our Solution (PASSED):** We wrote advanced OLAP queries (`sql/olap/cube_queries.sql`) mapping out business concepts like Year-Over-Year growth and rainfall correlation. These queries can be plugged directly into a visual dashboard (like Power BI or Tableau) so managers only have to look at charts.

## 6. "Data warehouse development is a massive effort"
**The Sin:** Trying to deliver the entire company's data in one 3-year "big bang" release.
**Our Solution (PASSED):** We started with an agile, iterative scope. We selected just 5 Regions and 5 Products to prove the pipeline from source to Mart, allowing us to deliver immediate value before scaling up to all of Kenya.

## 7. "Data warehouses are read-only"
**The Sin:** Failing to realize that a warehouse becomes the foundation for new data generation, like machine learning.
**Our Solution (PASSED):** We integrated a **Data Mining** layer (`src/mining/price_prediction.py`) that actively reads our processed data marts and generates new forward-looking predictive knowledge!
