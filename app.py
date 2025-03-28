import streamlit as st
import pandas as pd
import json
import os
from google.cloud import bigquery
from google.oauth2 import service_account

if os.path.exists("supplestore-957d0034398e.json"):
    # Local
    with open("supplestore-957d0034398e.json") as f:
        creds_dict = json.load(f)
else:
    # Streamlit Cloud
    creds_dict = json.loads(st.secrets["GOOGLE_APPLICATION_CREDENTIALS"])

credentials = service_account.Credentials.from_service_account_info(creds_dict)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Set Streamlit page configuration
st.set_page_config(page_title="SuppleScale Dashboard", layout="wide")
st.markdown("""
    <style>
    html, body, [class*="css"]  {
        font-size: 22px !important;
    }
    .stDataFrame th, .stDataFrame td {
        font-size: 18px !important;
    }
    .stMarkdown h1, h2, h3, h4 {
        font-size: 18px !important;
    }
    </style>
""", unsafe_allow_html=True)

st.title("📊 SuppleScale – BigQuery Supplement Analytics")

# Helper function to run queries
def run_query(query):
    query_job = client.query(query)
    return query_job.result().to_dataframe()

# Sidebar filters
st.sidebar.header("🔍 Filters")
region_filter = st.sidebar.multiselect(
    "Select Regions", ["West", "East", "South", "North"], default=["West", "East", "South", "North"]
)
date_range = st.sidebar.date_input("Select Date Range", [])

region_condition = f"AND c.region IN UNNEST({region_filter})" if region_filter else ""
date_condition = ""
if len(date_range) == 2:
    start_date = date_range[0].strftime('%Y-%m-%d')
    end_date = date_range[1].strftime('%Y-%m-%d')
    date_condition = f"AND f.order_date BETWEEN '{start_date}' AND '{end_date}'"

# Section 1: Revenue by Region
st.header("💰 Revenue by Region")
query_revenue = f"""
SELECT c.region, ROUND(SUM(f.revenue), 2) AS total_revenue
FROM supplestore.fact_orders f
JOIN supplestore.dim_customers c ON f.customer_id = c.customer_id
WHERE 1=1 {region_condition} {date_condition}
GROUP BY region
ORDER BY total_revenue DESC
"""
df_revenue = run_query(query_revenue)
st.bar_chart(df_revenue.set_index("region"))

# Section 2: Top Products by Revenue
st.header("🧴 Top Products by Revenue")
query_products = f"""
SELECT p.product_name, ROUND(SUM(f.revenue), 2) AS total_revenue
FROM supplestore.fact_orders f
JOIN supplestore.dim_products p ON f.product_id = p.product_id
WHERE 1=1 {date_condition}
GROUP BY product_name
ORDER BY total_revenue DESC
LIMIT 10
"""
df_products = run_query(query_products)
st.bar_chart(df_products.set_index("product_name"))

# Section 3: Top Customers by LTV
st.header("🏆 Top 10 Customers by Lifetime Value (LTV)")
query_ltv = f"""
SELECT
  f.customer_id,
  ROUND(SUM(f.revenue), 2) AS customer_ltv
FROM supplestore.fact_orders AS f
WHERE 1=1 {date_condition}
GROUP BY f.customer_id
ORDER BY customer_ltv DESC
LIMIT 10
"""
df_ltv = run_query(query_ltv)
st.dataframe(df_ltv)

# Section 4: Monthly Revenue Trend
st.header("📆 Monthly Revenue Trend")
query_trend = f"""
SELECT
  DATE_TRUNC(f.order_date, MONTH) AS month,
  ROUND(SUM(f.revenue), 2) AS monthly_revenue
FROM supplestore.fact_orders AS f
WHERE 1=1 {date_condition}
GROUP BY month
ORDER BY month

"""
df_trend = run_query(query_trend)
df_trend["month"] = pd.to_datetime(df_trend["month"])
st.line_chart(df_trend.set_index("month"))

# Section 5: Repeat Purchase Rate
st.header("🔁 Repeat Purchase Rate")
query_repeat = f"""
WITH customer_orders AS (
  SELECT
    customer_id,
    COUNT(f.order_id) AS order_count
  FROM supplestore.fact_orders AS f
  WHERE 1=1 {date_condition}
  GROUP BY customer_id
)

SELECT
  COUNT(*) AS total_customers,
  COUNTIF(order_count > 1) AS repeat_customers,
  ROUND(COUNTIF(order_count > 1) * 100.0 / COUNT(*), 2) AS repeat_rate
FROM customer_orders


"""
df_repeat = run_query(query_repeat)
st.metric("Total Customers", df_repeat['total_customers'][0])
st.metric("Repeat Customers", df_repeat['repeat_customers'][0])
st.metric("Repeat Rate (%)", df_repeat['repeat_rate'][0])

# Section 6: Cohort Analysis
st.header("👥 Cohort Analysis")
query_cohort = f"""
WITH customer_orders AS (
  SELECT
    customer_id,
    DATE_TRUNC(MIN(f.order_date), MONTH) AS cohort_month,
    DATE_TRUNC(f.order_date, MONTH) AS order_month
  FROM supplestore.fact_orders AS f
  WHERE 1=1 {date_condition}
  GROUP BY customer_id, f.order_date
),
cohort_counts AS (
  SELECT
    cohort_month,
    order_month,
    COUNT(DISTINCT customer_id) AS num_customers
  FROM customer_orders
  GROUP BY cohort_month, order_month
)

SELECT * FROM cohort_counts
ORDER BY cohort_month, order_month

"""
df_cohort = run_query(query_cohort)
st.dataframe(df_cohort)
