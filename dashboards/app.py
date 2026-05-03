import streamlit as st
import pandas as pd
import plotly.express as px
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv

# ── Page Configuration ──
st.set_page_config(page_title="AgriSupply Dashboard", layout="wide")
st.title("🌾 AgriSupply Executive Dashboard")
st.markdown("Interactive Business Intelligence pulling directly from the Data Warehouse Marts.")

# ── Data Loading ──
@st.cache_data
def load_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(base_dir, ".env"))

    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME", "postgres")

    url_object = URL.create(
        "postgresql",
        username=user,
        password=password,
        host=host,
        port=port,
        database=dbname,
    )
    engine = create_engine(url_object)
    
    try:
        df_market = pd.read_sql("SELECT * FROM mart_market", engine)
        df_supply = pd.read_sql("SELECT * FROM mart_supply", engine)
    except Exception as e:
        df_market = pd.DataFrame()
        df_supply = pd.DataFrame()
        st.error(f"Failed to query from PostgreSQL. Did you run the pipeline? Error: {e}")
        st.stop()
        
    if not df_market.empty:
        # Cast year and month to int to drop .0 floats, then create valid datetime objects
        df_market['year'] = df_market['year'].astype(int)
        df_market['month'] = df_market['month'].astype(int)
        df_market['date'] = pd.to_datetime(df_market['year'].astype(str) + '-' + df_market['month'].astype(str).str.zfill(2) + '-01')
    
    return df_market, df_supply

try:
    df_market, df_supply = load_data()
except Exception as e:
    st.error(f"Internal error loading data: {e}")
    st.stop()

# ── Sidebar Filters ──
st.sidebar.header("Filter Analytics")
selected_product = st.sidebar.selectbox("Select Product", options=["All"] + list(df_market["product_name"].unique()))
selected_region = st.sidebar.selectbox("Select Region", options=["All"] + list(df_market["region_name"].unique()))

# Apply Filters
filtered_market = df_market.copy()
filtered_supply = df_supply.copy()

if selected_product != "All":
    filtered_market = filtered_market[filtered_market["product_name"] == selected_product]
    filtered_supply = filtered_supply[filtered_supply["product_name"] == selected_product]
    
if selected_region != "All":
    filtered_market = filtered_market[filtered_market["region_name"] == selected_region]
    filtered_supply = filtered_supply[filtered_supply["region_name"] == selected_region]

# ── Top Level KPIs ──
col1, col2, col3 = st.columns(3)
with col1:
    avg_price = filtered_market["avg_price"].mean()
    st.metric("Avg Market Price (KES)", f"KES {avg_price:,.2f}" if pd.notnull(avg_price) else "N/A")
with col2:
    total_supply = filtered_supply["total_quantity"].sum()
    st.metric("Total Harvest Volume (Tons)", f"{total_supply:,.0f}" if pd.notnull(total_supply) else "N/A")
with col3:
    records = filtered_market["record_count"].sum()
    st.metric("Data Points Analyzed", f"{records:,}")

st.markdown("---")

# ── Visualizations ──
col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    st.subheader("Price Trends Over Time")
    if not filtered_market.empty:
        # Group gracefully based on the filters applied
        trend_data = filtered_market.groupby(['date', 'product_name'])['avg_price'].mean().reset_index()
        fig_line = px.line(trend_data, x="date", y="avg_price", color="product_name", 
                           title="Monthly Average Price (KES)",
                           labels={"date": "Date", "avg_price": "Price", "product_name": "Product"})
        st.plotly_chart(fig_line, use_container_width=True)
    else:
        st.warning("No data available for these filters.")

with col_chart2:
    st.subheader("Regional Supply Dominance")
    if not filtered_supply.empty:
        supply_pie = filtered_supply.groupby('region_name')['total_quantity'].sum().reset_index()
        fig_pie = px.pie(supply_pie, values="total_quantity", names="region_name",
                         title="Total Production by Region", hole=0.3)
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.warning("No data available for these filters.")

# ── Raw Data Peek ──
st.markdown("---")
st.subheader("Data Warehouse Mart (Filtered)")
st.dataframe(filtered_market.head(50), use_container_width=True)
