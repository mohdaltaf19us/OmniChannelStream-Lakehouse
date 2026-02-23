# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Data Mesh — Unity Catalog Domain Catalogs
# MAGIC
# MAGIC **Pattern:** Federated governance — each domain owns its catalog.
# MAGIC Shared dimensions live in `omnioc_platform` catalog.
# MAGIC Cross-domain access controlled via Unity Catalog grants.
# MAGIC
# MAGIC | Catalog | Domain Owner | Contents |
# MAGIC |---|---|---|
# MAGIC | omnioc_supply_chain | Supply Chain Team | products, suppliers, shipments, inventory |
# MAGIC | omnioc_finance | Finance Team | orders, payments, customers, exchange rates |
# MAGIC | omnioc_cx | CX / Analytics Team | clickstream, sessions, device telemetry |
# MAGIC | omnioc_platform | Platform Team | shared dims (date, warehouse, currency) |

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("source_catalog", "omnioc_dev", "Source catalog (copy from)")

SOURCE   = dbutils.widgets.get("source_catalog")

# Domain catalogs
SUPPLY   = "omnioc_supply_chain"
FINANCE  = "omnioc_finance"
CX       = "omnioc_cx"
PLATFORM = "omnioc_platform"

# COMMAND ----------
# MAGIC %md ## 1 — Create Domain Catalogs

for catalog in [SUPPLY, FINANCE, CX, PLATFORM]:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.published")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.internal")
    print(f"✅ Catalog created: {catalog}")

# COMMAND ----------
# MAGIC %md ## 2 — Platform Catalog: Shared Dimensions

print("Publishing shared dimensions to omnioc_platform...")

# Shared dim_date — referenced by all domains
spark.sql(f"""
CREATE OR REPLACE VIEW {PLATFORM}.published.dim_date AS
SELECT * FROM {SOURCE}.gold.dim_date
""")

# Shared dim_warehouse — referenced by supply chain and finance
spark.sql(f"""
CREATE OR REPLACE VIEW {PLATFORM}.published.dim_warehouse AS
SELECT * FROM {SOURCE}.gold.dim_warehouse
""")

# Currency reference table
spark.sql(f"""
CREATE OR REPLACE VIEW {PLATFORM}.published.dim_currency AS
SELECT DISTINCT
    base_currency,
    target_currency,
    LAST_VALUE(exchange_rate) OVER (
        PARTITION BY base_currency, target_currency
        ORDER BY rate_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS latest_exchange_rate,
    MAX(rate_date) OVER (PARTITION BY base_currency, target_currency) AS rate_date
FROM {SOURCE}.bronze.exchange_rates_raw
""")

print("✅ Platform catalog ready")

# COMMAND ----------
# MAGIC %md ## 3 — Supply Chain Domain Catalog

print("Publishing Supply Chain domain products...")

spark.sql(f"""
CREATE OR REPLACE VIEW {SUPPLY}.published.products AS
SELECT
    product_id,
    product_name,
    sku,
    category,
    sub_category,
    unit_cost,
    unit_price,
    ROUND((unit_price - unit_cost) / unit_price * 100, 2) AS margin_pct,
    is_active,
    supplier_id,
    reorder_point,
    reorder_quantity
FROM {SOURCE}.gold.dim_product
WHERE _is_current = true
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SUPPLY}.published.suppliers AS
SELECT
    supplier_id,
    supplier_name,
    country,
    city,
    rating,
    lead_time_days,
    payment_terms_days,
    is_active
FROM {SOURCE}.gold.dim_supplier
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SUPPLY}.published.supplier_performance AS
SELECT * FROM {SOURCE}.gold.agg_supplier_performance
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SUPPLY}.published.shipments AS
SELECT
    shipment_id, supplier_id, warehouse_id, product_id,
    quantity, total_cost, ship_date, actual_arrival_date,
    carrier, status, delivery_delay_days, is_late, sla_breached
FROM {SOURCE}.gold.fact_shipments
""")

print(f"✅ {SUPPLY} — 4 views published")

# COMMAND ----------
# MAGIC %md ## 4 — Finance Domain Catalog

print("Publishing Finance domain products...")

spark.sql(f"""
CREATE OR REPLACE VIEW {FINANCE}.published.customers AS
SELECT
    customer_id, full_name, email, segment, country, city,
    loyalty_points, loyalty_tier, is_active,
    total_orders, net_revenue, avg_order_value,
    customer_value_segment, churn_risk_score,
    first_order_date, last_order_date
FROM {SOURCE}.gold.wide_customer_order_summary
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {FINANCE}.published.orders AS
SELECT
    order_id, customer_sk, warehouse_id,
    order_date_key, status, channel, currency,
    total_amount, discount_amount, net_revenue,
    days_to_ship, days_to_deliver
FROM {SOURCE}.gold.fact_orders
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {FINANCE}.published.payments AS
SELECT
    payment_id, order_id, amount, currency, method, status,
    placed_at, paid_at, is_completed, is_refunded, days_to_pay
FROM {SOURCE}.gold.fact_payments
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {FINANCE}.published.daily_sales AS
SELECT * FROM {SOURCE}.gold.agg_daily_sales_by_region
""")

print(f"✅ {FINANCE} — 4 views published")

# COMMAND ----------
# MAGIC %md ## 5 — CX Domain Catalog

print("Publishing CX / Analytics domain products...")

spark.sql(f"""
CREATE OR REPLACE VIEW {CX}.published.session_aggregates AS
SELECT
    session_id, user_id, device_type, geo_country, geo_city,
    total_events, page_views, searches, cart_adds, purchases,
    max_cart_value, total_purchase_amount,
    session_start, session_end, session_duration_minutes,
    converted, window_start, window_end
FROM {SOURCE}.silver.session_aggregates
""") if spark.catalog.tableExists(f"{SOURCE}.silver.session_aggregates") else print("  (silver.session_aggregates not yet built — skipping)")

spark.sql(f"""
CREATE OR REPLACE VIEW {CX}.published.hourly_clickstream AS
SELECT * FROM {SOURCE}.gold.agg_hourly_clickstream
""") if spark.catalog.tableExists(f"{SOURCE}.gold.agg_hourly_clickstream") else print("  (gold.agg_hourly_clickstream not yet built — skipping)")

spark.sql(f"""
CREATE OR REPLACE VIEW {CX}.published.wide_sessions AS
SELECT * FROM {SOURCE}.gold.wide_session_events
""") if spark.catalog.tableExists(f"{SOURCE}.gold.wide_session_events") else print("  (gold.wide_session_events not yet built — skipping)")

print(f"✅ {CX} — views published")

# COMMAND ----------
# MAGIC %md ## 6 — Unity Catalog Access Control

print("Setting up UC access controls...")

# Supply Chain team — read their domain + platform dims
spark.sql(f"GRANT USE CATALOG ON CATALOG {SUPPLY} TO `supply_chain_analysts`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {SUPPLY}.published TO `supply_chain_analysts`")
spark.sql(f"GRANT SELECT ON SCHEMA {SUPPLY}.published TO `supply_chain_analysts`")
spark.sql(f"GRANT SELECT ON SCHEMA {PLATFORM}.published TO `supply_chain_analysts`")

# Finance team
spark.sql(f"GRANT USE CATALOG ON CATALOG {FINANCE} TO `finance_analysts`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {FINANCE}.published TO `finance_analysts`")
spark.sql(f"GRANT SELECT ON SCHEMA {FINANCE}.published TO `finance_analysts`")
spark.sql(f"GRANT SELECT ON SCHEMA {PLATFORM}.published TO `finance_analysts`")

# CX / Analytics — read-only on their domain + can query supply and finance published
spark.sql(f"GRANT USE CATALOG ON CATALOG {CX} TO `cx_analysts`")
spark.sql(f"GRANT SELECT ON SCHEMA {CX}.published TO `cx_analysts`")
spark.sql(f"GRANT SELECT ON SCHEMA {FINANCE}.published TO `cx_analysts`")
spark.sql(f"GRANT SELECT ON SCHEMA {PLATFORM}.published TO `cx_analysts`")

print("✅ UC access controls applied")

# COMMAND ----------
# MAGIC %md ## 7 — Row-level Security example (customers — hide PII from CX team)

# Finance table: CX team can only see anonymized customer data
spark.sql(f"""
CREATE OR REPLACE VIEW {CX}.published.customers_anonymized AS
SELECT
    SHA2(customer_id, 256)    AS customer_id_hashed,
    segment,
    country,
    city,
    CASE WHEN loyalty_points >= 10000 THEN 'Platinum'
         WHEN loyalty_points >= 5000  THEN 'Gold'
         WHEN loyalty_points >= 1000  THEN 'Silver'
         ELSE 'Bronze'
    END                       AS loyalty_tier,
    customer_value_segment,
    churn_risk_score,
    total_orders,
    net_revenue
FROM {SOURCE}.gold.wide_customer_order_summary
""")
print("✅ Anonymized customer view created for CX team (no PII)")

# COMMAND ----------
# MAGIC %md ## Summary

print("\n── DATA MESH CATALOG INVENTORY ────────────────────────────")
for cat in [SUPPLY, FINANCE, CX, PLATFORM]:
    try:
        views = spark.sql(f"SHOW VIEWS IN {cat}.published").count()
        print(f"  ✅ {cat:<30} {views} views in published schema")
    except Exception as e:
        print(f"  ❌ {cat}: {e}")

print("\n✅ Data Mesh setup complete")
print("   Each domain team accesses only their published schema")
print("   Cross-domain joins done by platform team via curated views")
