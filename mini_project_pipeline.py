"""
====================================================================================================
Data Engineering Practical 10: Mini Project
End-to-End Data Engineering Pipeline & Star Schema Data Warehouse Architecture
====================================================================================================

Authors:
- Aryan Rane (Roll No. 38 | Student ID: 5135872 | T.Y. Data Science)
- Om Bichave (Partner | T.Y. Data Science)

Architecture Flow:
[Multi-Source Ingestion (CSV / JSON / Stream)]
      |
      v
[Automated Cleaning & Auditing (Deduplication, Range Checks, Imputation)]
      |
      v
[Pipeline Transformation & Financial Enrichment (Gross/Net/Margin calculations)]
      |
      v
[Operational Data Store (ODS) Relational Staging (SQLite: ods_customers, ods_products, ods_orders)]
      |
      v
[Enterprise Data Warehouse - Star Schema Dimensional Modeling]
   - Dim_Customer (Surrogate customer_key, Customer profiling, Spending Tier)
   - Dim_Product  (Surrogate product_key, Product Catalog, Margins)
   - Dim_Date     (Surrogate date_key YYYYMMDD, Year, Quarter, Month, Day)
   - Fact_Sales   (Granular transactional metrics linked via foreign keys)
      |
      v
[Analytical OLAP Engine & Executive BI Reporting]
   - KPI 1: Executive Financial Overview
   - KPI 2: Category Profitability & Margin Ranking
   - KPI 3: Regional Revenue & Customer Tier Distribution
   - KPI 4: Monthly Sales Trend Analysis
====================================================================================================
"""

import os
import sqlite3
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

def run_pipeline(db_path: str = "enterprise_data_warehouse.db"):
    print("=" * 80)
    print("   DATA ENGINEERING MINI PROJECT — END-TO-END PIPELINE EXECUTION")
    print("   Project Team: Aryan Rane (Roll No. 38) & Om Bichave")
    print("=" * 80)

    # -------------------------------------------------------------------------
    # STEP 1: MULTI-SOURCE DATA INGESTION
    # -------------------------------------------------------------------------
    print("\n>>> [STEP 1] MULTI-SOURCE DATA INGESTION")
    np.random.seed(42)

    # Source 1: Customer Demographic Profile Feed (CRM)
    raw_customers = pd.DataFrame({
        "customer_id": [f"C{100+i}" for i in range(1, 11)],
        "customer_name": [
            "Alice Smith", "Bob Jones", "Charlie Brown", "David Lee", "Emma Watson",
            "Frank Miller", "Grace Hopper", "Henry Ford", "Ivy Taylor", "Jack Ryan"
        ],
        "city": [
            "Mumbai", "Pune", "Bangalore", "Mumbai", "Delhi",
            "Pune", "Hyderabad", "Bangalore", "Chennai", "Delhi"
        ],
        "signup_date": [
            "2025-01-15", "2025-02-20", "2025-03-10", "2025-04-05", "2025-05-12",
            "2025-06-18", "2025-07-22", "2025-08-30", "2025-09-14", "2025-10-01"
        ]
    })

    # Source 2: Enterprise Product Master Catalog
    raw_products = pd.DataFrame({
        "product_id": [f"P{200+i}" for i in range(1, 7)],
        "product_name": [
            "Ultra Laptop Pro", "Ergonomic Mouse", "Mechanical Keyboard",
            "4K Monitor 27inch", "USB-C Hub Multiport", "Noise-Cancelling Headphones"
        ],
        "category": [
            "Computers", "Accessories", "Accessories",
            "Displays", "Accessories", "Audio"
        ],
        "unit_price": [1200.0, 35.0, 85.0, 350.0, 45.0, 180.0],
        "unit_cost":  [850.0, 15.0, 40.0, 220.0, 20.0,  95.0]
    })

    # Source 3: Real-Time Transactional Order Feed (Simulating Batch Ingestion with Anomaly Injections)
    n_orders = 30
    order_dates = [
        datetime(2026, 1, 1) + timedelta(days=int(d))
        for d in np.random.randint(0, 90, n_orders)
    ]
    raw_orders = pd.DataFrame({
        "order_id": [f"ORD{1000+i}" for i in range(n_orders)],
        "customer_id": [f"C{100 + np.random.randint(1, 11)}" for _ in range(n_orders)],
        "product_id": [f"P{200 + np.random.randint(1, 7)}" for _ in range(n_orders)],
        "order_date": [d.strftime("%Y-%m-%d") for d in order_dates],
        "quantity": [np.random.randint(1, 6) for _ in range(n_orders)],
        "discount_pct": [np.random.choice([0.0, 0.05, 0.10, 0.15, 0.20]) for _ in range(n_orders)]
    })

    # Inject dirty data for automated audit verification:
    # 2 duplicate records, 1 negative quantity anomaly, 2 missing/null attributes
    raw_orders = pd.concat([raw_orders, raw_orders.iloc[[2]], raw_orders.iloc[[7]]], ignore_index=True)
    raw_orders.loc[4, "quantity"] = -3
    raw_orders.loc[12, "discount_pct"] = np.nan
    raw_orders.loc[18, "customer_id"] = np.nan

    print(f"Ingested Customers : {len(raw_customers)} records (CRM Feed)")
    print(f"Ingested Products  : {len(raw_products)} records (Catalog Catalog)")
    print(f"Ingested Orders    : {len(raw_orders)} records (Raw Transaction Stream with synthetic anomalies)")

    # -------------------------------------------------------------------------
    # STEP 2: DATA CLEANING & AUDITING
    # -------------------------------------------------------------------------
    print("\n>>> [STEP 2] DATA CLEANING & AUTOMATED AUDITING")
    initial_orders = len(raw_orders)

    # 1. Deduplication on natural key
    df_orders = raw_orders.drop_duplicates(subset=["order_id"]).copy()
    duplicates_dropped = initial_orders - len(df_orders)

    # 2. Filtering non-positive quantity anomalies
    invalid_qty_mask = df_orders["quantity"] <= 0
    anomalies_dropped = invalid_qty_mask.sum()
    df_orders = df_orders[~invalid_qty_mask].copy()

    # 3. Handling missing values
    df_orders = df_orders.dropna(subset=["customer_id"]).copy()
    df_orders["discount_pct"] = df_orders["discount_pct"].fillna(0.0)

    print(f"Duplicate records detected & removed : {duplicates_dropped}")
    print(f"Invalid negative quantity anomalies : {anomalies_dropped} (dropped)")
    print(f"Null values handled & imputed       : 2 (discount_pct defaulted to 0.0, null customer dropped)")
    print(f"Cleaned validated orders retained   : {len(df_orders)} records")

    # -------------------------------------------------------------------------
    # STEP 3: PIPELINE TRANSFORMATION & FINANCIAL FEATURE ENRICHMENT
    # -------------------------------------------------------------------------
    print("\n>>> [STEP 3] TRANSFORMATION & FINANCIAL FEATURE ENRICHMENT")
    df_transformed = df_orders.merge(
        raw_products[["product_id", "unit_price", "unit_cost"]],
        on="product_id",
        how="inner"
    )

    df_transformed["gross_amount"]    = (df_transformed["quantity"] * df_transformed["unit_price"]).round(2)
    df_transformed["discount_amount"] = (df_transformed["gross_amount"] * df_transformed["discount_pct"]).round(2)
    df_transformed["net_amount"]      = (df_transformed["gross_amount"] - df_transformed["discount_amount"]).round(2)
    df_transformed["total_cost"]      = (df_transformed["quantity"] * df_transformed["unit_cost"]).round(2)
    df_transformed["profit"]          = (df_transformed["net_amount"] - df_transformed["total_cost"]).round(2)

    sample_cols = ["order_id", "customer_id", "product_id", "quantity", "gross_amount", "discount_amount", "net_amount", "profit"]
    print("Enrichment complete. Sample transformed records:")
    print(df_transformed[sample_cols].head(5).to_string(index=False))

    # -------------------------------------------------------------------------
    # STEP 4: OPERATIONAL DATA STORE (ODS) RELATIONAL STAGING
    # -------------------------------------------------------------------------
    print("\n>>> [STEP 4] OPERATIONAL STORAGE (ODS DATABASE PERSISTENCE)")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    raw_customers.to_sql("ods_customers", conn, if_exists="replace", index=False)
    raw_products.to_sql("ods_products", conn, if_exists="replace", index=False)
    df_transformed.to_sql("ods_orders", conn, if_exists="replace", index=False)

    print(f"SQLite Database Initialized: {db_path}")
    print(f"  - ods_customers : {len(raw_customers)} records loaded")
    print(f"  - ods_products  : {len(raw_products)} records loaded")
    print(f"  - ods_orders    : {len(df_transformed)} records loaded")

    # -------------------------------------------------------------------------
    # STEP 5: ENTERPRISE DATA WAREHOUSE — STAR SCHEMA DIMENSIONAL MODELING
    # -------------------------------------------------------------------------
    print("\n>>> [STEP 5] DATA WAREHOUSE ARCHITECTURE — STAR SCHEMA DIMENSIONAL MODEL")
    cursor.executescript("""
    DROP TABLE IF EXISTS Fact_Sales;
    DROP TABLE IF EXISTS Dim_Customer;
    DROP TABLE IF EXISTS Dim_Product;
    DROP TABLE IF EXISTS Dim_Date;

    CREATE TABLE Dim_Customer (
        customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id TEXT UNIQUE,
        customer_name TEXT,
        city TEXT,
        tier TEXT
    );

    CREATE TABLE Dim_Product (
        product_key INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT UNIQUE,
        product_name TEXT,
        category TEXT,
        unit_price REAL,
        unit_cost REAL
    );

    CREATE TABLE Dim_Date (
        date_key INTEGER PRIMARY KEY,
        full_date TEXT UNIQUE,
        year INTEGER,
        quarter INTEGER,
        month INTEGER,
        month_name TEXT,
        day INTEGER,
        day_name TEXT
    );

    CREATE TABLE Fact_Sales (
        sales_key INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_key INTEGER,
        product_key INTEGER,
        date_key INTEGER,
        order_id TEXT,
        quantity INTEGER,
        gross_amount REAL,
        discount_amount REAL,
        net_amount REAL,
        profit REAL,
        FOREIGN KEY (customer_key) REFERENCES Dim_Customer(customer_key),
        FOREIGN KEY (product_key) REFERENCES Dim_Product(product_key),
        FOREIGN KEY (date_key) REFERENCES Dim_Date(date_key)
    );
    """)

    # Populate Dim_Customer with Customer Lifetime Value Tiering
    customer_spend = df_transformed.groupby("customer_id")["net_amount"].sum()
    def assign_tier(c_id):
        spend = customer_spend.get(c_id, 0)
        if spend > 2500:
            return "Gold Tier"
        elif spend > 1000:
            return "Silver Tier"
        else:
            return "Bronze Tier"

    dim_cust = raw_customers.copy()
    dim_cust["tier"] = dim_cust["customer_id"].apply(assign_tier)
    for _, r in dim_cust.iterrows():
        cursor.execute(
            "INSERT INTO Dim_Customer (customer_id, customer_name, city, tier) VALUES (?, ?, ?, ?)",
            (r["customer_id"], r["customer_name"], r["city"], r["tier"])
        )

    # Populate Dim_Product
    for _, r in raw_products.iterrows():
        cursor.execute(
            "INSERT INTO Dim_Product (product_id, product_name, category, unit_price, unit_cost) VALUES (?, ?, ?, ?, ?)",
            (r["product_id"], r["product_name"], r["category"], r["unit_price"], r["unit_cost"])
        )

    # Populate Dim_Date
    unique_dates = pd.to_datetime(df_transformed["order_date"].unique())
    for d in unique_dates:
        date_key = int(d.strftime("%Y%m%d"))
        full_date = d.strftime("%Y-%m-%d")
        cursor.execute("""
            INSERT INTO Dim_Date (date_key, full_date, year, quarter, month, month_name, day, day_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (date_key, full_date, d.year, d.quarter, d.month, d.strftime("%B"), d.day, d.strftime("%A")))

    # Dimensional Lookup ETL to populate central Fact_Sales
    dim_cust_df = pd.read_sql("SELECT customer_key, customer_id FROM Dim_Customer", conn)
    dim_prod_df = pd.read_sql("SELECT product_key, product_id FROM Dim_Product", conn)
    dim_date_df = pd.read_sql("SELECT date_key, full_date FROM Dim_Date", conn)

    fact_prep = (
        df_transformed
        .merge(dim_cust_df, on="customer_id", how="inner")
        .merge(dim_prod_df, on="product_id", how="inner")
        .merge(dim_date_df, left_on="order_date", right_on="full_date", how="inner")
    )

    fact_records = fact_prep[[
        "customer_key", "product_key", "date_key", "order_id",
        "quantity", "gross_amount", "discount_amount", "net_amount", "profit"
    ]]
    fact_records.to_sql("Fact_Sales", conn, if_exists="append", index=False)
    conn.commit()

    cust_count = cursor.execute("SELECT COUNT(*) FROM Dim_Customer").fetchone()[0]
    prod_count = cursor.execute("SELECT COUNT(*) FROM Dim_Product").fetchone()[0]
    date_count = cursor.execute("SELECT COUNT(*) FROM Dim_Date").fetchone()[0]
    fact_count = cursor.execute("SELECT COUNT(*) FROM Fact_Sales").fetchone()[0]

    print("Star Schema Dimensional Model Populated Successfully:")
    print(f"  - Dim_Customer : {cust_count} rows")
    print(f"  - Dim_Product  : {prod_count} rows")
    print(f"  - Dim_Date     : {date_count} rows")
    print(f"  - Fact_Sales   : {fact_count} rows")

    # -------------------------------------------------------------------------
    # STEP 6: BUSINESS INTELLIGENCE & ANALYTICAL OLAP REPORTING
    # -------------------------------------------------------------------------
    print("\n>>> [STEP 6] BUSINESS INTELLIGENCE & ANALYTICAL OLAP REPORTING")

    print("\n--- KPI 1: EXECUTIVE FINANCIAL OVERVIEW ---")
    kpi1_query = """
    SELECT 
        COUNT(sales_key) AS total_orders,
        SUM(quantity) AS total_units_sold,
        PRINTF('$%,.2f', SUM(gross_amount)) AS total_gross_sales,
        PRINTF('$%,.2f', SUM(discount_amount)) AS total_discounts,
        PRINTF('$%,.2f', SUM(net_amount)) AS total_net_revenue,
        PRINTF('$%,.2f', SUM(profit)) AS total_profit,
        PRINTF('%.2f%%', (SUM(profit) / SUM(net_amount)) * 100) AS overall_profit_margin
    FROM Fact_Sales;
    """
    print(pd.read_sql(kpi1_query, conn).to_string(index=False))

    print("\n--- KPI 2: CATEGORY PERFORMANCE & PROFITABILITY ---")
    kpi2_query = """
    SELECT 
        p.category,
        COUNT(f.sales_key) AS transactions,
        SUM(f.quantity) AS units_sold,
        PRINTF('$%,.2f', SUM(f.net_amount)) AS net_revenue,
        PRINTF('$%,.2f', SUM(f.profit)) AS net_profit,
        PRINTF('%.2f%%', (SUM(f.profit) / SUM(f.net_amount)) * 100) AS margin_pct
    FROM Fact_Sales f
    JOIN Dim_Product p ON f.product_key = p.product_key
    GROUP BY p.category
    ORDER BY SUM(f.net_amount) DESC;
    """
    print(pd.read_sql(kpi2_query, conn).to_string(index=False))

    print("\n--- KPI 3: REGIONAL REVENUE & CUSTOMER TIER BREAKDOWN ---")
    kpi3_query = """
    SELECT 
        c.city,
        c.tier,
        COUNT(DISTINCT c.customer_id) AS customer_count,
        SUM(f.quantity) AS total_items,
        PRINTF('$%,.2f', SUM(f.net_amount)) AS regional_revenue
    FROM Fact_Sales f
    JOIN Dim_Customer c ON f.customer_key = c.customer_key
    GROUP BY c.city, c.tier
    ORDER BY SUM(f.net_amount) DESC;
    """
    print(pd.read_sql(kpi3_query, conn).to_string(index=False))

    print("\n--- KPI 4: MONTHLY SALES TREND ANALYSIS ---")
    kpi4_query = """
    SELECT 
        d.year,
        d.month_name AS month,
        COUNT(f.sales_key) AS order_volume,
        SUM(f.quantity) AS units,
        PRINTF('$%,.2f', SUM(f.net_amount)) AS monthly_revenue,
        PRINTF('$%,.2f', AVG(f.net_amount)) AS avg_order_value
    FROM Fact_Sales f
    JOIN Dim_Date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
    ORDER BY d.year, d.month;
    """
    print(pd.read_sql(kpi4_query, conn).to_string(index=False))

    conn.close()
    print("\n" + "=" * 80)
    print("   [SUCCESS] End-to-End Pipeline Executed and Validated Successfully!")
    print("=" * 80)

if __name__ == "__main__":
    run_pipeline()
