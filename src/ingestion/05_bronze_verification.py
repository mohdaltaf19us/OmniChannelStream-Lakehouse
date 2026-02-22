# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Bronze Verification
# MAGIC Run after notebooks 01 → 04 to validate all 16 bronze tables.
# MAGIC Checks: row counts, audit columns, types, nulls, rescued data, domain metrics.

# COMMAND ----------
# DBTITLE 1, Config
from pyspark.sql.functions import col, max as spark_max

CATALOG = "omnioc_dev"

# All bronze tables — domain, source format, minimum expected rows
ALL_TABLES = {
    # Supply Chain
    "suppliers_raw"         : {"domain": "supply_chain", "source": "csv",       "expected_min": 500},
    "products_raw"          : {"domain": "supply_chain", "source": "csv",       "expected_min": 5000},
    "warehouses_raw"        : {"domain": "supply_chain", "source": "csv",       "expected_min": 20},
    "shipments_raw"         : {"domain": "supply_chain", "source": "parquet",   "expected_min": 30000},
    "supplier_contracts_raw": {"domain": "supply_chain", "source": "json",      "expected_min": 400},
    # Finance
    "customers_raw"         : {"domain": "finance",      "source": "json",      "expected_min": 10000},
    "orders_raw"            : {"domain": "finance",      "source": "json",      "expected_min": 100000},
    "order_lines_raw"       : {"domain": "finance",      "source": "json",      "expected_min": 50000},
    "payments_raw"          : {"domain": "finance",      "source": "json",      "expected_min": 40000},
    "refunds_raw"           : {"domain": "finance",      "source": "json",      "expected_min": 1000},
    "exchange_rates_raw"    : {"domain": "finance",      "source": "csv",       "expected_min": 2000},
    # Streaming — clickstream_raw_kafka is optional (only if Kafka path used)
    "clickstream_raw_eh"    : {"domain": "clickstream",  "source": "eventhubs", "expected_min": 100},
    "clickstream_raw_kafka" : {"domain": "clickstream",  "source": "kafka",     "expected_min": 0},
    # CDC
    "cdc_customers_raw"     : {"domain": "cdc",          "source": "json",      "expected_min": 1000},
    "cdc_products_raw"      : {"domain": "cdc",          "source": "json",      "expected_min": 200},
    # DQ Testing
    "poison_pills_raw"      : {"domain": "dq_testing",   "source": "json",      "expected_min": 100},
}

print(f"✅ Catalog : {CATALOG}")
print(f"✅ Tables  : {len(ALL_TABLES)}")

# COMMAND ----------
# MAGIC %md ## 1 — Row Counts & Column Counts

# COMMAND ----------
# DBTITLE 1, Row counts from every table
results = {}
errors  = []

print(f"  {'Table':<35} {'Rows':>10}  {'Cols':>5}  {'Expected ≥':>10}  Status")
print("─" * 75)

for t, meta in ALL_TABLES.items():
    full = f"{CATALOG}.bronze.{t}"
    try:
        df   = spark.table(full)
        rows = df.count()
        cols = len(df.columns)
        exp  = meta["expected_min"]
        results[t] = {"rows": rows, "cols": cols, "df": df, "meta": meta}

        if exp == 0:
            status = "✅ OPTIONAL"
        elif rows == 0:
            status = "❌ EMPTY"
            errors.append(t)
        elif rows < exp:
            status = "⚠️  LOW"
        else:
            status = "✅"

        print(f"  {t:<35} {rows:>10,}  {cols:>5}  {exp:>10,}  {status}")

    except Exception as e:
        print(f"  {t:<35} {'—':>10}  {'—':>5}  {'—':>10}  ❌ NOT FOUND")
        errors.append(t)

print("─" * 75)
if errors:
    print(f"\n  ⚠️  {len(errors)} table(s) missing/empty — run ingestion notebooks first: {errors}")
else:
    print(f"\n  ✅ All tables present")

# COMMAND ----------
# MAGIC %md ## 2 — Audit Columns Check

# COMMAND ----------
# DBTITLE 1, Verify _ingest_time, _source_system, _source_file on every table
print(f"  {'Table':<35}  _ingest_time   _source_system   _source_file")
print("─" * 80)

for t, data in results.items():
    cols = data["df"].columns
    it   = "✅" if "_ingest_time"   in cols else "❌"
    ss   = "✅" if "_source_system" in cols else "❌"
    # streaming tables don't have _source_file (no file path from Event Hubs)
    sf   = "✅" if "_source_file" in cols else "— streaming"
    print(f"  {t:<35}  {it:<15}{ss:<17}{sf}")

# COMMAND ----------
# MAGIC %md ## 3 — Latest Ingest Time

# COMMAND ----------
# DBTITLE 1, Most recent _ingest_time — confirms freshness
print(f"  {'Table':<35}  Latest _ingest_time")
print("─" * 70)
for t, data in results.items():
    df = data["df"]
    if "_ingest_time" in df.columns and data["rows"] > 0:
        latest = df.select(spark_max("_ingest_time")).collect()[0][0]
        print(f"  {t:<35}  {str(latest)}")

# COMMAND ----------
# MAGIC %md ## 4 — Source System Tags

# COMMAND ----------
# DBTITLE 1, Distinct _source_system values per table
print(f"  {'Table':<35}  _source_system")
print("─" * 65)
for t, data in results.items():
    df = data["df"]
    if "_source_system" in df.columns and data["rows"] > 0:
        vals = [r[0] for r in df.select("_source_system").distinct().collect()]
        print(f"  {t:<35}  {vals}")

# COMMAND ----------
# MAGIC %md ## 5 — Type Verification

# COMMAND ----------
# DBTITLE 1, suppliers_raw — rating=double, is_active=boolean
spark.table(f"{CATALOG}.bronze.suppliers_raw").select(
    "supplier_id","rating","lead_time_days","payment_terms_days","is_active"
).printSchema()

# COMMAND ----------
# DBTITLE 1, products_raw — unit_cost=double, reorder_point=int, is_active=boolean
spark.table(f"{CATALOG}.bronze.products_raw").select(
    "product_id","unit_cost","unit_price","weight_kg","reorder_point","reorder_quantity","is_active"
).printSchema()

# COMMAND ----------
# DBTITLE 1, orders_raw — amounts=double
spark.table(f"{CATALOG}.bronze.orders_raw").select(
    "order_id","total_amount","discount_amount","tax_amount","shipping_amount"
).printSchema()

# COMMAND ----------
# DBTITLE 1, order_lines_raw — quantity=int, prices=double, is_gift=boolean
spark.table(f"{CATALOG}.bronze.order_lines_raw").select(
    "line_id","quantity","unit_price","discount_pct","line_total","is_gift"
).printSchema()

# COMMAND ----------
# DBTITLE 1, shipments_raw — quantity=int, costs=double
spark.table(f"{CATALOG}.bronze.shipments_raw").select(
    "shipment_id","quantity","unit_cost","total_cost"
).printSchema()

# COMMAND ----------
# MAGIC %md ## 6 — Null Rate on Key ID Columns

# COMMAND ----------
# DBTITLE 1, Null % on primary key columns
KEY_COLS = {
    "suppliers_raw"         : "supplier_id",
    "products_raw"          : "product_id",
    "warehouses_raw"        : "warehouse_id",
    "shipments_raw"         : "shipment_id",
    "supplier_contracts_raw": "contract_id",
    "customers_raw"         : "customer_id",
    "orders_raw"            : "order_id",
    "order_lines_raw"       : "line_id",
    "payments_raw"          : "payment_id",
    "refunds_raw"           : "refund_id",
    "exchange_rates_raw"    : "rate_date",
    "clickstream_raw_eh"    : "event_id",
    "cdc_customers_raw"     : "cdc_event_id",
    "cdc_products_raw"      : "cdc_event_id",
    "poison_pills_raw"      : "order_id",    # nulls expected here — intentional DQ test data
}

print(f"  {'Table':<35}  {'Key Column':<25}  Nulls   Total      Null%")
print("─" * 80)
for t, key_col in KEY_COLS.items():
    if t not in results or results[t]["rows"] == 0:
        continue
    df    = results[t]["df"]
    total = results[t]["rows"]
    nulls = df.filter(col(key_col).isNull()).count()
    pct   = nulls / total * 100
    flag  = "⚠️ " if pct > 1 and t != "poison_pills_raw" else "✅"
    print(f"  {flag} {t:<33}  {key_col:<25}  {nulls:>5}  {total:>8,}  {pct:>6.1f}%")

# COMMAND ----------
# MAGIC %md ## 7 — Rescued Data Check

# COMMAND ----------
# DBTITLE 1, Rows where _rescued_data is not null (schema drift captured by Autoloader)
print(f"  {'Table':<35}  Rescued   Total      %")
print("─" * 65)
for t, data in results.items():
    df = data["df"]
    if "_rescued_data" not in df.columns or data["rows"] == 0:
        continue
    total   = data["rows"]
    rescued = df.filter("_rescued_data is not null").count()
    pct     = rescued / total * 100
    flag    = "⚠️ " if rescued > 0 else "✅"
    print(f"  {flag} {t:<33}  {rescued:>7}  {total:>8,}  {pct:>5.1f}%")

# COMMAND ----------
# MAGIC %md ## 8 — Domain Metrics

# COMMAND ----------
# DBTITLE 1, Order status breakdown
print("  orders_raw — status:")
spark.table(f"{CATALOG}.bronze.orders_raw").groupBy("status").count().orderBy("count", ascending=False).show()

# COMMAND ----------
# DBTITLE 1, Payment status breakdown
print("  payments_raw — status:")
spark.table(f"{CATALOG}.bronze.payments_raw").groupBy("status").count().orderBy("count", ascending=False).show()

# COMMAND ----------
# DBTITLE 1, Shipment status and carrier
print("  shipments_raw — status:")
spark.table(f"{CATALOG}.bronze.shipments_raw").groupBy("status").count().orderBy("count", ascending=False).show()

print("  shipments_raw — carrier:")
spark.table(f"{CATALOG}.bronze.shipments_raw").groupBy("carrier").count().orderBy("count", ascending=False).show()

# COMMAND ----------
# DBTITLE 1, CDC operations breakdown
print("  cdc_customers_raw — operation:")
spark.table(f"{CATALOG}.bronze.cdc_customers_raw").groupBy("operation").count().orderBy("operation").show()

print("  cdc_products_raw — operation:")
spark.table(f"{CATALOG}.bronze.cdc_products_raw").groupBy("operation").count().orderBy("operation").show()

# COMMAND ----------
# DBTITLE 1, Poison pills — DQ issue types (all 5 types expected)
print("  poison_pills_raw — _dq_issue:")
spark.table(f"{CATALOG}.bronze.poison_pills_raw").groupBy("_dq_issue").count().orderBy("count", ascending=False).show()

# COMMAND ----------
# DBTITLE 1, Clickstream DQ flags — late arrivals and poison pills
if "clickstream_raw_eh" in results and results["clickstream_raw_eh"]["rows"] > 0:
    df    = spark.table(f"{CATALOG}.bronze.clickstream_raw_eh")
    total = df.count()
    late  = df.filter(col("_is_late_arrival") == True).count()
    pills = df.filter(col("_is_poison_pill")  == True).count()
    print(f"  clickstream_raw_eh:")
    print(f"    Total          : {total:,}")
    print(f"    Late arrivals  : {late:,}  ({late/total*100:.1f}%)")
    print(f"    Poison pills   : {pills:,}  ({pills/total*100:.1f}%)")
else:
    print("  clickstream_raw_eh — no data (run 01_bronze_clickstream_eventhub.py first)")

# COMMAND ----------
# MAGIC %md ## 9 — Final Pass / Fail Summary

# COMMAND ----------
# DBTITLE 1, Final verdict — all tables
passed   = 0
failed   = 0
optional = 0

print("\n" + "="*80)
print("  05 Bronze Verification — Final Verdict")
print("="*80)
print(f"  {'Table':<35}  {'Rows':>10}  {'Domain':<15}  Verdict")
print("─"*80)

for t, meta in ALL_TABLES.items():
    domain   = meta["domain"]
    expected = meta["expected_min"]

    if t not in results:
        print(f"  ❌  {t:<33}  {'—':>10}  {domain:<15}  MISSING — table not found")
        failed += 1
        continue

    rows = results[t]["rows"]

    if expected == 0:
        verdict = "✅ OPTIONAL"
        optional += 1
    elif rows >= expected:
        verdict = "✅ PASS"
        passed += 1
    elif rows > 0:
        verdict = "⚠️  LOW"
        passed += 1
    else:
        verdict = "❌ FAIL"
        failed += 1

    print(f"  {t:<35}  {rows:>10,}  {domain:<15}  {verdict}")

print("="*80)
print(f"  Passed   : {passed}")
print(f"  Failed   : {failed}")
print(f"  Optional : {optional}")
print("="*80)

if failed == 0:
    print("\n  ✅ All required bronze tables verified — ready to build Silver layer")
else:
    print(f"\n  ❌ {failed} table(s) failed — re-run ingestion notebooks for failed tables")
