"""
generate_raw_data.py
--------------------
Generates realistic raw CSV files for the AgriSupply Data Warehouse demo.

Targets:
  market_prices.csv  -> ~50,000 rows (5 products x 5 regions x daily)
  weather.csv        -> ~10,000 rows (5 regions x daily)
  production.csv     -> ~5,000 rows  (5 products x 5 regions x every-10-days)

Date range: 2020-01-01 to 2025-12-31 (~2,192 days -> 6 years)
"""

import csv
import random
import math
import os
from datetime import date, timedelta

random.seed(42)

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR  = os.path.join(BASE_DIR, "data", "raw")

MARKET_FILE    = os.path.join(RAW_DIR, "market_prices", "market_prices.csv")
WEATHER_FILE   = os.path.join(RAW_DIR, "weather",       "weather.csv")
PRODUCTION_FILE= os.path.join(RAW_DIR, "production",    "production.csv")

# ── Master lists ──────────────────────────────────────────────────────────────
PRODUCTS = ["maize", "beans", "tomatoes", "potatoes", "wheat"]
REGIONS  = ["Nairobi", "Eldoret", "Kisumu", "Meru", "Nakuru"]

# ── Date range ────────────────────────────────────────────────────────────────
START = date(2020, 1, 1)
END   = date(2025, 12, 31)

def date_range(start, end, step=1):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=step)

# ── Region climate profiles ───────────────────────────────────────────────────
# (base_temp_C, temp_variation, wet_season_months, avg_rainfall_mm)
CLIMATE = {
    "Nairobi": dict(base_temp=22.0, variation=3.0, wet_months=[3,4,5,10,11], rain_avg=12),
    "Eldoret": dict(base_temp=18.0, variation=3.5, wet_months=[3,4,5,8,9,10], rain_avg=15),
    "Kisumu":  dict(base_temp=27.0, variation=2.5, wet_months=[3,4,5,10,11], rain_avg=14),
    "Meru":    dict(base_temp=21.0, variation=3.0, wet_months=[3,4,10,11], rain_avg=18),
    "Nakuru":  dict(base_temp=19.5, variation=3.0, wet_months=[3,4,5,10,11], rain_avg=10),
}

# ── Product price profiles (KES per unit) ─────────────────────────────────────
# (base_price, seasonal_amplitude, region_factor, annual_drift_per_year)
PRICE_PROFILE = {
    "maize":    dict(base=50,  amp=8,  drift=2.5),
    "beans":    dict(base=100, amp=12, drift=3.0),
    "tomatoes": dict(base=80,  amp=15, drift=4.0),
    "potatoes": dict(base=60,  amp=10, drift=2.0),
    "wheat":    dict(base=70,  amp=8,  drift=3.5),
}

REGION_PRICE_FACTOR = {
    "Nairobi": 1.08,
    "Eldoret": 0.93,
    "Kisumu":  0.98,
    "Meru":    0.95,
    "Nakuru":  0.90,
}

# ── Production profiles (qty in metric tons) ──────────────────────────────────
PROD_PROFILE = {
    "maize":    dict(base=1000, amp=200),
    "beans":    dict(base=500,  amp=150),
    "tomatoes": dict(base=800,  amp=300),
    "potatoes": dict(base=700,  amp=200),
    "wheat":    dict(base=600,  amp=150),
}

REGION_PROD_FACTOR = {
    "Nairobi": 0.95,
    "Eldoret": 1.10,
    "Kisumu":  1.00,
    "Meru":    1.05,
    "Nakuru":  1.08,
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def seasonal_wave(month, peak_month=8):
    """Returns a value in [-1, 1] peaking at peak_month."""
    angle = 2 * math.pi * (month - peak_month) / 12
    return math.cos(angle)

def years_elapsed(d):
    return (d - START).days / 365.25

def generate_price(product, region, d):
    p   = PRICE_PROFILE[product]
    rf  = REGION_PRICE_FACTOR[region]
    yr  = years_elapsed(d)
    trend = p["base"] + p["drift"] * yr
    seasonal = p["amp"] * seasonal_wave(d.month, peak_month=7)
    noise = random.gauss(0, p["base"] * 0.03)
    price = (trend + seasonal + noise) * rf
    return round(max(price, 5.0), 2)

def generate_temp(region, d):
    c = CLIMATE[region]
    seasonal = c["variation"] * seasonal_wave(d.month, peak_month=7)  # cooler mid-year
    noise = random.gauss(0, 1.2)
    return round(c["base_temp"] - seasonal + noise, 1)

def generate_rainfall(region, d):
    c = CLIMATE[region]
    if d.month in c["wet_months"]:
        # wet season: random rainfall events
        if random.random() < 0.45:
            return round(random.expovariate(1 / c["rain_avg"]) * 2, 1)
    else:
        if random.random() < 0.08:
            return round(random.expovariate(1 / (c["rain_avg"] * 0.3)), 1)
    return 0.0

def generate_quantity(product, region, d):
    p  = PROD_PROFILE[product]
    rf = REGION_PROD_FACTOR[region]
    # harvest peaks Aug-Oct
    seasonal = p["amp"] * seasonal_wave(d.month, peak_month=9)
    noise = random.gauss(0, p["base"] * 0.05)
    qty = (p["base"] + seasonal + noise) * rf
    return max(int(qty), 50)


# ── Generators ────────────────────────────────────────────────────────────────

def generate_market_prices():
    print("Generating market_prices.csv ...")
    rows = 0
    with open(MARKET_FILE, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "product", "region", "price"])
        for d in date_range(START, END):
            for product in PRODUCTS:
                for region in REGIONS:
                    w.writerow([d.isoformat(), product, region,
                                generate_price(product, region, d)])
                    rows += 1
    print(f"  -> {rows:,} rows written to {MARKET_FILE}")
    return rows

def generate_weather():
    print("Generating weather.csv ...")
    rows = 0
    with open(WEATHER_FILE, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "region", "temperature", "rainfall"])
        for d in date_range(START, END):
            for region in REGIONS:
                w.writerow([d.isoformat(), region,
                             generate_temp(region, d),
                             generate_rainfall(region, d)])
                rows += 1
    print(f"  -> {rows:,} rows written to {WEATHER_FILE}")
    return rows

def generate_production():
    print("Generating production.csv ...")
    rows = 0
    with open(PRODUCTION_FILE, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "product", "region", "quantity"])
        # snapshot every 10 days to hit ~5,000 rows
        for d in date_range(START, END, step=10):
            for product in PRODUCTS:
                for region in REGIONS:
                    w.writerow([d.isoformat(), product, region,
                                generate_quantity(product, region, d)])
                    rows += 1
    print(f"  -> {rows:,} rows written to {PRODUCTION_FILE}")
    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("  AgriSupply Data Warehouse — Raw Data Generator")
    print("=" * 55)

    mp = generate_market_prices()
    wt = generate_weather()
    pr = generate_production()

    print()
    print("Summary")
    print("-" * 35)
    print(f"  market_prices.csv : {mp:>8,} rows")
    print(f"  weather.csv       : {wt:>8,} rows")
    print(f"  production.csv    : {pr:>8,} rows")
    print(f"  TOTAL             : {mp+wt+pr:>8,} rows")
    print()
    print("Done.")
