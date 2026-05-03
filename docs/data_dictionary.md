# Data Dictionary

## Overview
This dictionary describes every field in the AgriSupply Data Warehouse — raw sources, staging outputs, and warehouse tables.

---

## Raw Layer

### market_prices.csv
| Column  | Type    | Nullable | Description                      | Valid Range / Values                          |
|--------|---------|----------|----------------------------------|-----------------------------------------------|
| date   | DATE    | No       | Transaction date (YYYY-MM-DD)    | 2020-01-01 to 2025-12-31                      |
| product| VARCHAR | No       | Crop name (lowercase in raw)     | maize, beans, tomatoes, potatoes, wheat       |
| region | VARCHAR | No       | Administrative region            | Nairobi, Eldoret, Kisumu, Meru, Nakuru        |
| price  | DECIMAL | No       | Market price in KES per unit     | > 0, expected 5–500 KES                       |

### weather.csv
| Column      | Type    | Nullable | Description                    | Valid Range / Values                   |
|------------|---------|----------|--------------------------------|----------------------------------------|
| date       | DATE    | No       | Observation date (YYYY-MM-DD)  | 2020-01-01 to 2025-12-31               |
| region     | VARCHAR | No       | Administrative region          | Nairobi, Eldoret, Kisumu, Meru, Nakuru |
| temperature| DECIMAL | No       | Avg daily temperature (°C)     | 5.0 to 40.0                            |
| rainfall   | DECIMAL | No       | Daily rainfall (mm)            | >= 0.0                                 |

### production.csv
| Column   | Type    | Nullable | Description                        | Valid Range / Values                          |
|---------|---------|----------|------------------------------------|-----------------------------------------------|
| date    | DATE    | No       | Snapshot date (YYYY-MM-DD)         | 2020-01-01 to 2025-12-31                      |
| product | VARCHAR | No       | Crop name (lowercase in raw)       | maize, beans, tomatoes, potatoes, wheat       |
| region  | VARCHAR | No       | Administrative region              | Nairobi, Eldoret, Kisumu, Meru, Nakuru        |
| quantity| INTEGER | No       | Quantity produced (metric tons)    | >= 50                                         |

---

## Staging Layer

### stg_market_prices
| Column  | Type    | Description                             |
|--------|---------|-----------------------------------------|
| date   | DATE    | Validated transaction date              |
| product| VARCHAR | Standardized product name (title case)  |
| region | VARCHAR | Validated region name                   |
| price  | DECIMAL | Validated price (KES)                   |

### stg_weather
| Column      | Type    | Description                           |
|------------|---------|---------------------------------------|
| date       | DATE    | Validated observation date            |
| region     | VARCHAR | Validated region name                 |
| temperature| DECIMAL | Validated temperature (°C)            |
| rainfall   | DECIMAL | Validated rainfall (mm)               |

### stg_production
| Column   | Type    | Description                              |
|---------|---------|------------------------------------------|
| date    | DATE    | Validated snapshot date                  |
| product | VARCHAR | Standardized product name (title case)   |
| region  | VARCHAR | Validated region name                    |
| quantity| INTEGER | Validated quantity (metric tons)         |

---

## Warehouse Layer

### fact_agri
| Column      | Type    | Description                          |
|------------|---------|--------------------------------------|
| date_id    | INTEGER | FK → dim_date.date_id                |
| product_id | INTEGER | FK → dim_product.product_id          |
| region_id  | INTEGER | FK → dim_region.region_id            |
| price      | DECIMAL | Market price of product (KES)        |
| quantity   | INTEGER | Production quantity (metric tons)    |
| rainfall   | DECIMAL | Daily rainfall at region (mm)        |
| temperature| DECIMAL | Daily temperature at region (°C)     |

### dim_date
| Column   | Type    | Description         |
|---------|---------|---------------------|
| date_id | INTEGER | Surrogate PK        |
| date    | DATE    | Full date           |
| day     | INTEGER | Day of month (1–31) |
| month   | INTEGER | Month number (1–12) |
| year    | INTEGER | 4-digit year        |
| quarter | INTEGER | Quarter (1–4)       |
| week    | INTEGER | ISO week number     |

### dim_product
| Column       | Type    | Description          |
|-------------|---------|----------------------|
| product_id  | INTEGER | Surrogate PK         |
| product_name| VARCHAR | Product display name |

**Seed data:**
| product_id | product_name |
|-----------|-------------|
| 1         | Maize       |
| 2         | Beans       |
| 3         | Tomatoes    |
| 4         | Potatoes    |
| 5         | Wheat       |

### dim_region
| Column      | Type    | Description           |
|------------|---------|------------------------|
| region_id  | INTEGER | Surrogate PK           |
| region_name| VARCHAR | Region display name    |
| country    | VARCHAR | Country name           |

**Seed data:**
| region_id | region_name | country |
|----------|-------------|---------|
| 1        | Nairobi     | Kenya   |
| 2        | Eldoret     | Kenya   |
| 3        | Kisumu      | Kenya   |
| 4        | Meru        | Kenya   |
| 5        | Nakuru      | Kenya   |

---

## Data Mart Layer

### mart_market (Market Mart)
| Column       | Type    | Description                          |
|-------------|---------|--------------------------------------|
| date        | DATE    | Reporting date                       |
| product_name| VARCHAR | Product name                         |
| region_name | VARCHAR | Region name                          |
| avg_price   | DECIMAL | Average market price for period      |
| min_price   | DECIMAL | Minimum price                        |
| max_price   | DECIMAL | Maximum price                        |
| price_delta | DECIMAL | Price change vs. prior period        |

### mart_supply (Supply Mart)
| Column        | Type    | Description                         |
|--------------|---------|-------------------------------------|
| date         | DATE    | Reporting date                      |
| product_name | VARCHAR | Product name                        |
| region_name  | VARCHAR | Region name                         |
| total_qty    | INTEGER | Total production quantity           |
| avg_qty      | DECIMAL | Average production quantity         |
| qty_rank     | INTEGER | Regional rank by quantity           |
