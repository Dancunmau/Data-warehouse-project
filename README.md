# AgriSupply Data Warehouse

![AgriSupply MDS Architecture](docs/images/mds_architecture.png)

## Overview
AgriSupply is a demo **Modern Data Stack (MDS)** data warehouse for agricultural analytics across Kenya. It integrates daily market prices, weather observations, and production data into a fully implemented star schema warehouse built natively in **PostgreSQL** using **dbt (Data Build Tool)**.

## Current Status — Fully Implemented ELT 

| Layer         | Status | Output |
|--------------|--------|--------|
| Data generation |  Done | 71,260 raw CSV rows across 3 sources |
| Extract & Load |  Done | Python SQLAlchemy natively loads CSVs to DB |
| dbt Staging |  Done | `stg_*` views perform validation & cleaning |
| dbt Warehouse |  Done | 54,800 rows populated into `fact_agri` table |
| dbt Marts |  Done | Market & Supply aggregates pushed to schema |
| OLAP queries |  Done | PostgreSQL native CUBE & ROLLUP queries |
| Pipeline runner |  Done | `pipelines/run_pipeline.py` triggers ELT |

---

## Run the Full Pipeline

Make sure PostgreSQL is running and your `.env` is configured with `DB_PASSWORD`.

```bash
# Run the pipeline (Extract, Load, and Transform all at once)
python pipelines/run_pipeline.py

# Generate dbt documentation and view data lineage
# (In PowerShell, set your DB_PASSWORD first)
$env:DB_PASSWORD="@Dancunmau2004"
dbt docs generate --project-dir dbt_project --profiles-dir dbt_project
dbt docs serve --project-dir dbt_project --profiles-dir dbt_project --port 8081
```

---

## Project Structure

```
AgriSupply Data Warehouse/
├── dashboards/         app.py (Streamlit BI connected to DB)
├── data/
│   └── raw/            (Only Raw CSV data resides here)
├── dbt_project/
│   ├── models/
│   │   ├── staging/    stg_market_prices.sql, sources.yml
│   │   ├── warehouse/  fact_agri.sql, dim_product.sql
│   │   └── marts/      mart_market.sql, mart_supply.sql
│   └── dbt_project.yml
├── docs/               architecture, schemas, pipeline flow diagrams
├── pipelines/          run_pipeline.py (Master Orchestrator)
├── scripts/            generate_raw_data.py
├── sql/                metadata and OLAP cube query support files
└── src/
    ├── elt/            extract_load_db.py (EL runner)
    ├── mining/         price_prediction.py (Predictive model on Postgres)
    └── metadata/       elt_logger.py (Postgres native logging)
```

---

## Tech Stack
- **Python** — raw data generation, orchestration, AI data mining, Streamlit
- **SQLAlchemy** — high performance native ingestion into Data Warehouse
- **dbt (Data Build Tool)** — SQL transformation framework for analytics engineering
- **PostgreSQL** — core unified relational analytical storage
- **Jinja** — macro logic within dbt for dynamic table generation

## Architecture
```
Raw CSV → [SQLAlchemy EL] → PostgreSQL → [dbt T] → Warehouse Star Schema → Marts → Dashboards
```
