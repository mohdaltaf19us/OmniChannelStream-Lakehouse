# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Bronze Autoloader — JSON Files
# MAGIC
# MAGIC **Actual ADLS paths (container=landing, subfolder=landing/):**
# MAGIC - `landing/landing/finance/customers/customers.json`
# MAGIC - `landing/landing/finance/orders/orders.json`
# MAGIC - `landing/landing/finance/order_lines/order_lines.json`
# MAGIC - `landing/landing/finance/payments/payments.json`
# MAGIC - `landing/landing/finance/refunds/refunds.json`
# MAGIC - `landing/landing/supply_chain/supplier_contracts/supplier_contracts.json`
# MAGIC - `landing/landing/cdc/customers/cdc_customers_changes.json`
# MAGIC - `landing/landing/cdc/products/cdc_products_changes.json`
# MAGIC - `landing/landing/bad_data/poison_pills_orders.json`
# MAGIC
# MAGIC **Writes to:** `omnioc_dev.bronze.*_raw` via Autoloader trigger-once (batch mode)

# COMMAND ----------
# DBTITLE 1, Paths & Shared Config

# Container = "landing"
# Files are inside the landing/landing/... subfolder
LANDING = "abfss://landing@omniocdeveusst01.dfs.core.windows.net/landing"
CHKPT   = "abfss://checkpoint@omniocdeveusst01.dfs.core.windows.net/bronze"

# ── Autoloader options applied to every JSON source ───────────────────────────
JSON_OPTIONS = {
    # --- Format ---
    "cloudFiles.format"             : "json",

    # --- Schema handling ---
    "cloudFiles.inferColumnTypes"   : "true",          # infer types (not everything as string)
    "cloudFiles.schemaEvolutionMode": "addNewColumns",  # auto-add new fields from future files
    # cloudFiles.schemaLocation set per table inside load_json()

    # --- Rescue ---
    "cloudFiles.rescuedDataColumn"  : "_rescued_data", # any unparseable field stored here

    # --- Corruption handling ---
    "columnNameOfCorruptRecord"     : "_corrupt_record",# store corrupt rows
    "mode"                          : "PERMISSIVE",     # don't fail on bad rows
}

# JSON-lines (one object per line) — default, used for all finance/cdc/bad_data files
JSONL_OPTIONS = {**JSON_OPTIONS, "multiLine": "false"}

# Multi-line JSON (array in one file) — used for supplier_contracts
MULTI_OPTIONS = {**JSON_OPTIONS, "multiLine": "true"}

print("✅ Config ready")
print(f"   LANDING : {LANDING}")
print(f"   CHKPT   : {CHKPT}")

# COMMAND ----------
from pyspark.sql.functions import current_timestamp, lit, input_file_name

def load_json(source_path, target_table, multiline=False, extra_options=None):
    """
    Load JSON folder into a Delta bronze table using Autoloader (trigger once = batch mode).
    Adds metadata cols: _ingest_time, _source_file, _source_system.

    Args:
        source_path  : full abfss path to folder (must end with /)
        target_table : fully-qualified Delta table e.g. omnioc_dev.bronze.orders_raw
        multiline    : True for multi-line JSON array files (supplier_contracts)
        extra_options: additional cloudFiles options dict
    """
    table_name = target_table.split(".")[-1]
    schema_loc = f"{CHKPT}/schema/{table_name}"    # Autoloader persists inferred schema here
    chkpt_loc  = f"{CHKPT}/{table_name}"            # stream offset — tracks which files processed

    base_opts = MULTI_OPTIONS if multiline else JSONL_OPTIONS
    options   = {**base_opts, "cloudFiles.schemaLocation": schema_loc}
    if extra_options:
        options.update(extra_options)

    print(f"  → Source    : {source_path}")
    print(f"  → Multiline : {multiline}")
    print(f"  → Schema    : {schema_loc}")
    print(f"  → Checkpoint: {chkpt_loc}")

    df = (spark.readStream
        .format("cloudFiles")
        .options(**options)
        .load(source_path)
        .withColumn("_ingest_time",   current_timestamp())
        .withColumn("_source_file",   input_file_name())
        .withColumn("_source_system", lit("adls_json"))
    )

    (df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", chkpt_loc)
        .option("mergeSchema",        "true")  # handles schema drift gracefully
        .trigger(availableNow=True)            # batch mode: process all new files then stop
        .toTable(target_table)
        .awaitTermination()
    )

    count = spark.table(target_table).count()
    print(f"  ✅ {target_table} → {count:,} rows\n")

# COMMAND ----------
# MAGIC %md ## Finance Tables

# COMMAND ----------
# DBTITLE 1, customers_raw
load_json(
    source_path  = f"{LANDING}/finance/customers/",
    target_table = "omnioc_dev.bronze.customers_raw",
)

# COMMAND ----------
# DBTITLE 1, orders_raw
load_json(
    source_path  = f"{LANDING}/finance/orders/",
    target_table = "omnioc_dev.bronze.orders_raw",
)

# COMMAND ----------
# DBTITLE 1, order_lines_raw
load_json(
    source_path  = f"{LANDING}/finance/order_lines/",
    target_table = "omnioc_dev.bronze.order_lines_raw",
)

# COMMAND ----------
# DBTITLE 1, payments_raw
load_json(
    source_path  = f"{LANDING}/finance/payments/",
    target_table = "omnioc_dev.bronze.payments_raw",
)

# COMMAND ----------
# DBTITLE 1, refunds_raw
load_json(
    source_path  = f"{LANDING}/finance/refunds/",
    target_table = "omnioc_dev.bronze.refunds_raw",
)

# COMMAND ----------
# MAGIC %md ## Supply Chain Tables

# COMMAND ----------
# DBTITLE 1, supplier_contracts_raw  (multiline=True — single JSON array file)
load_json(
    source_path  = f"{LANDING}/supply_chain/supplier_contracts/",
    target_table = "omnioc_dev.bronze.supplier_contracts_raw",
    multiline    = True,   # supplier_contracts.json is a JSON array, not JSON-lines
)

# COMMAND ----------
# MAGIC %md ## CDC Tables

# COMMAND ----------
# DBTITLE 1, cdc_customers_raw
load_json(
    source_path  = f"{LANDING}/cdc/customers/",
    target_table = "omnioc_dev.bronze.cdc_customers_raw",
)

# COMMAND ----------
# DBTITLE 1, cdc_products_raw
load_json(
    source_path  = f"{LANDING}/cdc/products/",
    target_table = "omnioc_dev.bronze.cdc_products_raw",
)

# COMMAND ----------
# MAGIC %md ## Bad Data Table

# COMMAND ----------
# DBTITLE 1, poison_pills_raw
load_json(
    source_path  = f"{LANDING}/bad_data/",
    target_table = "omnioc_dev.bronze.poison_pills_raw",
)

# COMMAND ----------
# DBTITLE 1, Summary
print("\n" + "="*65)
print("  03 Bronze JSON — Load Summary")
print("="*65)
tables = {
    "customers_raw"         : 10000,
    "orders_raw"            : 100000,
    "order_lines_raw"       : 50000,
    "payments_raw"          : 40000,
    "refunds_raw"           : 1000,
    "supplier_contracts_raw": 400,
    "cdc_customers_raw"     : 1000,
    "cdc_products_raw"      : 200,
    "poison_pills_raw"      : 100,
}
all_ok = True
for tbl, expected in tables.items():
    count  = spark.table(f"omnioc_dev.bronze.{tbl}").count()
    status = "✅" if count >= expected else "⚠️ LOW"
    if count == 0: status = "❌ EMPTY"; all_ok = False
    print(f"  {status}  {tbl:<35} {count:>8,} rows  (expected ≥ {expected:,})")
print("="*65)
print(f"  {'✅ All tables loaded' if all_ok else '❌ Some tables empty — check paths above'}")
print("="*65)
