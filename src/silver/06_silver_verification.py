# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Silver — Verification
# MAGIC
# MAGIC Run after all silver transforms complete.
# MAGIC Validates row counts, nullability, types, dedup, and SCD2 integrity.

# COMMAND ----------
# DBTITLE 1, Cell 1 — Config
from pyspark.sql.functions import col, count, countDistinct, max as spark_max, min as spark_min

SILVER = "omnioc_dev.silver"

TABLES = {
    "supply_chain" : [
        ("suppliers",           "supplier_id"),
        ("products",            "product_id"),
        ("warehouses",          "warehouse_id"),
        ("shipments",           "shipment_id"),
        ("supplier_contracts",  "contract_id"),
    ],
    "finance" : [
        ("customers",     "customer_id"),
        ("orders",        "order_id"),
        ("order_lines",   "line_id"),
        ("payments",      "payment_id"),
        ("refunds",       "refund_id"),
        ("exchange_rates","rate_id"),
    ],
    "cdc" : [
        ("customers_scd", "customer_id"),
        ("products_scd",  "product_id"),
    ],
    "streaming" : [
        ("clickstream",             "event_id"),
        ("device_telemetry",        "telemetry_id"),
        ("order_events",            "event_id"),
        ("order_event_line_items",  "event_id"),
        ("dq_alerts",               "alert_id"),
        ("poison_pills",            "record_id"),
    ],
}

print("✅ Config ready — run cells in order")

# COMMAND ----------
# DBTITLE 1, Cell 2 — Row counts across all 20 tables
print(f"{'':=<72}")
print("  Silver Layer — Row Count Summary")
print(f"{'':=<72}")

total_tables = 0
total_rows   = 0

for domain, tables in TABLES.items():
    print(f"\n  {domain.upper()}")
    print(f"  {'─'*60}")
    for tbl, pk in tables:
        df   = spark.table(f"{SILVER}.{tbl}")
        rows = df.count()
        status = "✅" if rows > 0 else "❌"
        print(f"  {status}  {tbl:<30}  {rows:>8,}")
        total_tables += 1
        total_rows   += rows

print(f"\n{'':=<72}")
print(f"  Total tables : {total_tables}")
print(f"  Total rows   : {total_rows:,}")
print(f"{'':=<72}")

# COMMAND ----------
# DBTITLE 1, Cell 3 — Primary key null check (all tables)
print("── Primary Key Null Check ───────────────────────────────────────────")
all_pass = True
for domain, tables in TABLES.items():
    for tbl, pk in tables:
        df     = spark.table(f"{SILVER}.{tbl}")
        nulls  = df.filter(col(pk).isNull()).count()
        status = "✅" if nulls == 0 else "❌"
        if nulls > 0:
            all_pass = False
        print(f"  {status}  {tbl:<30}  {pk:<20}  nulls: {nulls}")

print(f"\n  {'✅ All PKs non-null' if all_pass else '❌ NULL PKs found — investigate'}")

# COMMAND ----------
# DBTITLE 1, Cell 4 — Duplicate check (all non-SCD tables)
print("── Duplicate Primary Key Check (non-SCD tables) ────────────────────")
all_pass = True
for domain, tables in TABLES.items():
    if domain == "cdc":
        continue    # SCD tables intentionally have multiple rows per PK
    for tbl, pk in tables:
        if tbl == "order_event_line_items":
            continue    # composite key — skip simple PK dedup check
        df    = spark.table(f"{SILVER}.{tbl}")
        dupes = (
            df.groupBy(pk).count()
            .filter(col("count") > 1)
            .count()
        )
        status = "✅" if dupes == 0 else "❌"
        if dupes > 0:
            all_pass = False
        print(f"  {status}  {tbl:<30}  duplicate PKs: {dupes}")

print(f"\n  {'✅ No duplicates found' if all_pass else '❌ Duplicates found — check dedup logic'}")

# COMMAND ----------
# DBTITLE 1, Cell 5 — SCD Type 2 integrity check
print("── SCD Type 2 Integrity ─────────────────────────────────────────────")

for tbl, pk in [("customers_scd", "customer_id"), ("products_scd", "product_id")]:
    df      = spark.table(f"{SILVER}.{tbl}")
    total   = df.count()
    current = df.filter(col("_is_current") == True).count()
    history = total - current

    # No business key should have more than one current row
    dupes = (
        df.filter(col("_is_current") == True)
        .groupBy(pk).count()
        .filter(col("count") > 1)
        .count()
    )

    # All closed rows should have _valid_to populated
    open_closed = df.filter(
        (col("_is_current") == False) & col("_valid_to").isNull()
    ).count()

    print(f"\n  {tbl}")
    print(f"    Total versions           : {total:,}")
    print(f"    Current (_is_current=T)  : {current:,}")
    print(f"    Historical               : {history:,}")
    print(f"    Duplicate current rows   : {dupes}   {'✅' if dupes == 0 else '❌'}")
    print(f"    Closed rows missing _valid_to : {open_closed}   {'✅' if open_closed == 0 else '❌'}")

# COMMAND ----------
# DBTITLE 1, Cell 6 — Timestamp type check (spot check key columns)
print("── Timestamp Column Type Check ─────────────────────────────────────")

TIMESTAMP_CHECKS = [
    ("clickstream",      "event_time"),
    ("device_telemetry", "event_time"),
    ("order_events",     "event_time"),
    ("dq_alerts",        "alert_time"),
    ("suppliers",        "created_at"),
    ("customers",        "created_at"),
    ("orders",           "order_date"),
    ("payments",         "payment_date"),
]

for tbl, ts_col in TIMESTAMP_CHECKS:
    df       = spark.table(f"{SILVER}.{tbl}")
    col_type = dict(df.dtypes)[ts_col]
    ok       = col_type in ("timestamp", "date")
    print(f"  {'✅' if ok else '❌'}  {tbl:<25}  {ts_col:<20}  type: {col_type}")

# COMMAND ----------
# DBTITLE 1, Cell 7 — Streaming freshness check
print("── Streaming Silver — Freshness ────────────────────────────────────")

for tbl in ["clickstream", "device_telemetry", "order_events", "dq_alerts"]:
    df     = spark.table(f"{SILVER}.{tbl}")
    latest = df.select(spark_max("_silver_ingest_time")).collect()[0][0]
    oldest = df.select(spark_min("_silver_ingest_time")).collect()[0][0]
    rows   = df.count()
    print(f"  {tbl}")
    print(f"    Rows    : {rows:,}")
    print(f"    Earliest: {oldest}")
    print(f"    Latest  : {latest}")

# COMMAND ----------
# DBTITLE 1, Cell 8 — Order event line items check
print("── order_event_line_items — Explode Validation ─────────────────────")

df_events    = spark.table(f"{SILVER}.order_events")
df_line_items = spark.table(f"{SILVER}.order_event_line_items")

events_with_items = df_events.filter(col("line_item_count") > 0).count()
total_line_items  = df_line_items.count()
unique_events     = df_line_items.select("event_id").distinct().count()
null_products     = df_line_items.filter(col("product_id").isNull()).count()
negative_totals   = df_line_items.filter(col("line_total") < 0).count()

print(f"  Order events with line items : {events_with_items:,}")
print(f"  Total exploded line item rows: {total_line_items:,}")
print(f"  Unique event_ids in lines    : {unique_events:,}")
print(f"  NULL product_id rows         : {null_products}   {'✅' if null_products == 0 else '❌'}")
print(f"  Negative line_total rows     : {negative_totals}   {'✅' if negative_totals == 0 else '❌'}")

df_line_items.select(
    "event_id", "order_id", "line_index",
    "product_id", "quantity", "unit_price", "discount_pct", "line_total"
).show(5, truncate=60)

# COMMAND ----------
# DBTITLE 1, Cell 9 — Poison pills summary
print("── Poison Pills — Quarantine Summary ───────────────────────────────")

df_pp = spark.table(f"{SILVER}.poison_pills")
total = df_pp.count()
print(f"  Total quarantined : {total:,}")
df_pp.groupBy("source_topic").count().orderBy("count", ascending=False).show()

# COMMAND ----------
# DBTITLE 1, Cell 10 — Final pass/fail summary
print(f"{'':=<72}")
print("  Silver Verification — Final Summary")
print(f"{'':=<72}")

checks = {}

for domain, tables in TABLES.items():
    for tbl, pk in tables:
        df   = spark.table(f"{SILVER}.{tbl}")
        rows = df.count()
        checks[tbl] = "✅" if rows > 0 else "❌ EMPTY"

for tbl, result in checks.items():
    print(f"  {result}  {tbl}")

failed = [t for t, r in checks.items() if "❌" in r]
print(f"\n{'':=<72}")
if not failed:
    print("  ✅ All silver tables populated — proceed to Gold layer")
else:
    print(f"  ❌ {len(failed)} tables failed: {', '.join(failed)}")
print(f"{'':=<72}")
