# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Bronze Verification
# MAGIC Checks all Bronze tables: row counts, DQ metrics, Delta history, audit column presence.

# COMMAND ----------
from pyspark.sql import functions as F
import sys

dbutils.widgets.text("catalog", "omnioc_dev", "Unity Catalog")
CATALOG = dbutils.widgets.get("catalog")

ALL_TABLES = [
    # Supply chain
    "suppliers_raw", "products_raw", "warehouses_raw", "shipments_raw",
    "supplier_contracts_raw",
    # Finance
    "customers_raw", "orders_raw", "order_lines_raw",
    "payments_raw", "refunds_raw", "exchange_rates_raw",
    # Streaming
    "clickstream_raw",
    # CDC
    "cdc_customers_raw", "cdc_products_raw",
    # DQ
    "poison_pills_raw",
]

# COMMAND ----------
# MAGIC %md ## 1 — Row counts & column count

print(f"{'Table':<40} {'Rows':>12}  {'Cols':>5}  Status")
print("─" * 68)
results = {}
errors  = []
for t in ALL_TABLES:
    full = f"{CATALOG}.bronze.{t}"
    try:
        df = spark.table(full)
        rows = df.count()
        cols = len(df.columns)
        results[t] = {"rows": rows, "cols": cols, "df": df}
        print(f"{full:<40} {rows:>12,}  {cols:>5}  ✅")
    except Exception as e:
        errors.append((t, str(e)))
        print(f"{full:<40} {'—':>12}  {'—':>5}  ❌ {str(e)[:40]}")

if errors:
    print(f"\n⚠️  {len(errors)} table(s) missing — run ingestion notebooks first")

# COMMAND ----------
# MAGIC %md ## 2 — Audit columns check

REQUIRED_AUDIT = ["_source_file", "_ingest_time", "_source"]
print("Audit column check:")
for t, data in results.items():
    df = data["df"]
    missing = [c for c in REQUIRED_AUDIT if c not in df.columns]
    status = "✅" if not missing else f"❌ missing: {missing}"
    print(f"  {t:<40} {status}")

# COMMAND ----------
# MAGIC %md ## 3 — DQ metrics for clickstream (late arrivals, poison pills)

if "clickstream_raw" in results:
    df = results["clickstream_raw"]["df"]
    total = results["clickstream_raw"]["rows"]
    if total > 0:
        late   = df.filter(F.col("_is_late_arrival") == True).count()
        poison = df.filter(F.col("_is_poison_pill")  == True).count()
        failed = df.filter(F.col("_parse_failed")    == True).count()
        print(f"\nClickstream DQ:")
        print(f"  Total records  : {total:,}")
        print(f"  Late arrivals  : {late:,} ({late/total*100:.1f}%)")
        print(f"  Poison pills   : {poison:,} ({poison/total*100:.1f}%)")
        print(f"  Parse failures : {failed:,} ({failed/total*100:.1f}%)")

# COMMAND ----------
# MAGIC %md ## 4 — Rescued data check (schema drift)

print("\nRescued data check (schema drift / extra fields):")
for t, data in results.items():
    df = data["df"]
    if "_rescued_data" in df.columns:
        rescued = df.filter(F.col("_rescued_data").isNotNull()).count()
        print(f"  {t:<40} rescued rows: {rescued:,}")

# COMMAND ----------
# MAGIC %md ## 5 — Delta table history (last 3 operations per table)

print("\nDelta table history (last 3 ops):")
for t in ["orders_raw", "clickstream_raw", "customers_raw"]:
    full = f"{CATALOG}.bronze.{t}"
    try:
        hist = spark.sql(f"DESCRIBE HISTORY {full} LIMIT 3")
        display(hist.select("version", "timestamp", "operation", "operationParameters"))
    except Exception as e:
        print(f"  {t}: {e}")

# COMMAND ----------
# MAGIC %md ## 6 — Assert minimum rows (CI/CD gate)

MIN_ROWS = {
    "suppliers_raw":  100,
    "products_raw":   500,
    "orders_raw":     1000,
    "customers_raw":  500,
}

print("\nMinimum row assertions:")
all_passed = True
for t, min_r in MIN_ROWS.items():
    actual = results.get(t, {}).get("rows", 0)
    passed = actual >= min_r
    all_passed = all_passed and passed
    print(f"  {'✅' if passed else '❌'} {t:<35} {actual:>8,} >= {min_r:>6,}")

if not all_passed:
    raise AssertionError("❌ Bronze verification FAILED — minimum row counts not met")

print("\n✅ All Bronze verification checks passed!")
