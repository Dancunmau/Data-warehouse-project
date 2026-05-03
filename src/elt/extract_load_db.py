"""
extract_load_db.py
------------------
The 'EL' in our modern ELT pipeline.
This script replaces pythonic transformations completely. 
It simply reads Raw data components and aggressively pushes them
untouched into PostgreSQL as literal raw tables using SQLAlchemy.
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(BASE_DIR, ".env"))

def extract_and_load():
    print("=" * 60)
    print("  PHASE 1: Extract & Load (Raw -> PostgreSQL)")
    print("=" * 60)
    
    # Authenticate to local PostgreSQL
    user = os.environ.get("DB_USER", "postgres")
    password = os.environ.get("DB_PASSWORD", "")
    host = os.environ.get("DB_HOST", "localhost")
    port = os.environ.get("DB_PORT", "5432")
    dbname = os.environ.get("DB_NAME", "postgres")
    
    if password == "your_password_here" or not password:
        print("[ERROR] Please update DB_PASSWORD in .env!")
        return

    from sqlalchemy.engine import URL
    
    # Create SQLAlchemy Engine safely (handles passwords with @ symbols!)
    url_object = URL.create(
        "postgresql",
        username=user,
        password=password,
        host=host,
        port=port,
        database=dbname,
    )
    engine = create_engine(url_object)
    
    # Target files
    raw_dir = os.path.join(BASE_DIR, "data", "raw")
    sources = {
        "raw_market_prices": os.path.join(raw_dir, "market_prices", "market_prices.csv"),
        "raw_weather": os.path.join(raw_dir, "weather", "weather.csv"),
        "raw_production": os.path.join(raw_dir, "production", "production.csv")
    }

    try:
        # Loop through CSVs and push them blindly into the DB
        for table_name, filepath in sources.items():
            if not os.path.exists(filepath):
                print(f"Skipping {table_name}: File not found.")
                continue
                
            print(f"Extracting {os.path.basename(filepath)}...")
            df = pd.read_csv(filepath)
            
            print(f"Loading into PostgreSQL table: {table_name}...")
            # to_sql handles the DROP/CREATE natively on 'replace'
            df.to_sql(table_name, engine, if_exists="replace", index=False)
            print(f"  -> Successfully landed {len(df)} rows natively into database staging.\n")
            
        print("EL Phase Complete! The database now owns the raw data.")
            
    except Exception as e:
        print(f"\n[ERROR] Failed to load data to PostgreSQL: {e}")

if __name__ == '__main__':
    extract_and_load()
