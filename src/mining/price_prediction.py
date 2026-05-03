"""
price_prediction.py
-------------------
Data Mining Module for the AgriSupply Warehouse.
This script demonstrates how Data Scientists consume the 
Warehouse `fact_agri` and `mart_market` layers to run 
predictive analytics.

It calculates a 3-month Moving Average (MA) to predict 
future product prices, mimicking basic demand forecasting.
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def get_db_engine():
    load_dotenv(os.path.join(BASE_DIR, ".env"))
    return create_engine(URL.create(
        "postgresql",
        username=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "postgres"),
    ))

def run_price_prediction(target_product="Maize", target_region="Nairobi"):
    engine = get_db_engine()

    # 1. Extract historical data from the Database directly
    historical_prices = []
    
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            query = text("SELECT year, month, avg_price FROM mart_market WHERE product_name = :p AND region_name = :r")
            result = conn.execute(query, {"p": target_product, "r": target_region})
            for row in result:
                historical_prices.append({
                    "year": int(row[0]),
                    "month": int(row[1]),
                    "avg_price": float(row[2])
                })
    except Exception as e:
        print(f"Failed to query database. Error: {e}")
        return
                
    if not historical_prices:
        print(f"No historical data found for {target_product} in {target_region}.")
        return

    # Sort chronological
    historical_prices.sort(key=lambda x: (x["year"], x["month"]))

    # 2. Extract recent trend (last 3 months)
    recent_data = historical_prices[-3:]
    if len(recent_data) < 3:
        print("Not enough history to generate a forecast.")
        return
        
    prices = [m["avg_price"] for m in recent_data]
    moving_average = round(sum(prices) / 3, 2)
    last_year, last_month = recent_data[-1]["year"], recent_data[-1]["month"]
    
    # Calculate next month
    next_month = last_month + 1 if last_month < 12 else 1
    next_year = last_year if last_month < 12 else last_year + 1

    # 3. Print the Data Mining Forecast
    print(f"\n{'='*55}")
    print(f"  Data Mining: Predictive Price Forecast")
    print(f"{'='*55}")
    print(f"  Target: {target_product} in {target_region}")
    print(f"  Recent Historical Prices:")
    for month_data in recent_data:
        print(f"    - {month_data['year']}-{month_data['month']:02d}: {month_data['avg_price']} KES")
    
    print(f"\n  [FORECAST] PREDICTED PRICE FOR {next_year}-{next_month:02d}: {moving_average} KES")
    print(f"{'='*55}\n")

if __name__ == "__main__":
    # Simulate a Data Scientist running the model
    run_price_prediction()
    
    # Run another prediction
    run_price_prediction(target_product="Beans", target_region="Nakuru")
