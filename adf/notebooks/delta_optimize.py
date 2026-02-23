# Databricks notebook source
# MAGIC %md
# MAGIC # Maintenance: Delta OPTIMIZE + Z-ORDER + VACUUM
# MAGIC Called by ADF pipeline PL_09_Delta_Optimize (weekly Sunday 03:00 UTC).
# MAGIC Also usable interactively for ad-hoc maintenance.

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("catalog",          "omnioc_dev", "Unity Catalog")
dbutils.widgets.text("layer",            "gold",       "Layer: bronze | silver | gold | all")
dbutils.widgets.text("vacuum",           "true",       "Run VACUUM after OPTIMIZE?")
dbutils.widgets.text("retention_hours",  "168",        "VACUUM retention hours (min 168)")

CATALOG         = dbutils.widgets.get("catalog")
LAYER           = dbutils.widgets.get("layer")
RUN_VACUUM      = dbutils.widgets.get("vacuum").lower() == "true"
RETENTION_HOURS = int(dbutils.widgets.get("retention_hours"))

assert RETENTION_HOURS >= 168, "Retention must be >= 168 hours (Delta safety minimum)"

# Z-ORDER column map — most common filter/join columns per table
ZORDER_MAP = {
    # Gold — optimised for dashboard queries
    "fact_orders":                  ["order_date_key", "customer_sk"],
    "fact_payments":                ["placed_at"],
    "fact_shipments":               ["ship_date", "supplier_id"],
    "dim_customer":                 ["customer_id"],
    "dim_product":                  ["product_id", "category"],
    "agg_daily_sales_by_region":    ["order_date", "region"],
    "agg_supplier_performance":     ["supplier_id"],
    "wide_customer_order_summary":  ["customer_id", "customer_value_segment"],
    "wide_product_performance":     ["product_id", "category", "velocity_band"],
    # Silver — optimised for DLT reads
    "orders":                       ["order_date", "customer_id"],
    "customers":                    ["customer_id"],
    "products":                     ["product_id"],
    "shipments":                    ["ship_date", "supplier_id"],
    # Bronze — no Z-ORDER (raw data, high churn)
}

results = []

def optimize_table(schema, table):
    full = f"{CATALOG}.{schema}.{table}"
    zorder_cols = ZORDER_MAP.get(table, [])
    try:
        if zorder_cols:
            zorder_str = ", ".join(zorder_cols)
            spark.sql(f"OPTIMIZE {full} ZORDER BY ({zorder_str})")
            action = f"OPTIMIZE + ZORDER({zorder_str})"
        else:
            spark.sql(f"OPTIMIZE {full}")
            action = "OPTIMIZE"

        if RUN_VACUUM:
            spark.sql(f"VACUUM {full} RETAIN {RETENTION_HOURS} HOURS")
            action += f" + VACUUM({RETENTION_HOURS}h)"

        # Get table stats post-optimize
        detail = spark.sql(f"DESCRIBE DETAIL {full}").first()
        results.append({
            "table": full, "status": "✅ OK", "action": action,
            "num_files": detail["numFiles"] if detail else 0,
            "size_gb": round(detail["sizeInBytes"] / 1e9, 3) if detail else 0,
        })
    except Exception as e:
        results.append({"table": full, "status": f"❌ {str(e)[:60]}", "action": "", "num_files": 0, "size_gb": 0})

# COMMAND ----------
# MAGIC %md ## Get tables for selected layer(s)

layers = ["bronze", "silver", "gold"] if LAYER == "all" else [LAYER]

for schema in layers:
    try:
        tables = [r["tableName"] for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect()]
        print(f"\n── {schema.upper()} ({len(tables)} tables) ────────────────")
        for t in tables:
            print(f"  Processing: {t}")
            optimize_table(schema, t)
    except Exception as e:
        print(f"  ❌ Could not list {CATALOG}.{schema}: {e}")

# COMMAND ----------
# MAGIC %md ## Results Summary

print(f"\n{'Table':<60} {'Status':<15} {'Files':>8}  {'Size GB':>8}  Action")
print("─" * 110)
for r in results:
    print(f"{r['table']:<60} {r['status']:<15} {r['num_files']:>8,}  {r['size_gb']:>8.3f}  {r['action']}")

ok    = sum(1 for r in results if "✅" in r["status"])
fails = sum(1 for r in results if "❌" in r["status"])
print(f"\n✅ {ok} tables optimised   ❌ {fails} errors")

if fails > 0:
    raise ValueError(f"❌ {fails} table(s) failed OPTIMIZE — review output above")
print("✅ Delta maintenance complete")
