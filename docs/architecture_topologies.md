# ELT vs ETL and Schema Topologies

## 1. ETL vs. ELT Architecture
### The Migration
In this project, we originally built a legacy **ETL** (Extract, Transform, Load) architecture. We extracted raw CSV data, transformed it entirely inside pure Python (`ingest_raw.py` and `transform_load.py`), and then produced finalized "Load-ready" Data Warehouse CSVs.

We have since completely refactored the pipeline into the modern **ELT** (Extract, Load, Transform) architecture using **SQLAlchemy and dbt**.

### Why ELT?
In modern cloud data warehousing (like Snowflake or BigQuery), compute is extremely cheap and scalable. Thus, the industry has shifted heavily to ELT. 
- **Extract & Load:** We dump all the messy raw CSV data directly into raw PostgreSQL tables *without* cleaning it first (`src/etl/extract_load_db.py`).
- **Transform:** We use **dbt (Data Build Tool)** to natively clean, join, and structure the data entirely within the database using pure SQL, moving the heavy-lifting off our local machine and onto the robust database engine. 

*Our AgriSupply Pipeline utilizes dbt views (`stg_market_prices`) and tables (`fact_agri`) to instantly compile data directly from the raw database schemas into advanced analytics logic!*

---

## 2. Schema Topologies: Star vs Snowflake

You astutely noticed the distinction between Star and Snowflake schemas! For this project, you requested we stick to the **Star Schema**. Here is why that was the right choice.

### Star Schema (What we built)
- **Design:** The `fact_agri` table sits in the absolute center. It points directly to `dim_product`, `dim_region`, and `dim_date`. It looks like a simple 3-point star.
- **Pros:** Query speeds are extremely fast because there is only *one* SQL `JOIN` required to fetch region names or product names against our metrics. It is highly denormalized and analyst friendly.
- **Example:** `dim_region` holds `(region_id, region_name, country)`.

### Snowflake Schema
- **Design:** The dimension tables are heavily normalized (split apart). The star "fractures" outwards like a snowflake.
- **Example:** `fact_agri` points to `dim_region`. But `dim_region` only holds `(region_id, region_name, country_id)`. We then have to join `dim_region` to a completely new table called `dim_country` `(country_id, country_name)`. 
- **Cons:** It saves a tiny amount of disk space, but analytical queries now require 2 or 3 chained `JOINs` just to answer simple questions, destroying performance. Modern data warehouses like ours almost always prefer Star Schemas!
