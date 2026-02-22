# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Bronze Autoloader — CSV Files
# MAGIC
# MAGIC **Actual ADLS paths (container=landing, subfolder=landing/):**
# MAGIC - `landing/landing/supply_chain/suppliers/suppliers.csv`
# MAGIC - `landing/landing/supply_chain/products/products.csv`
# MAGIC - `landing/landing/supply_chain/warehouses/warehouses.csv`
# MAGIC - `landing/landing/finance/exchange_rates/exchange_rates.csv`
# MAGIC
# MAGIC **Writes to:** `omnioc_dev.bronze.*_raw` via Autoloader trigger-once (batch mode)

# COMMAND ----------
# DBTITLE 1, Paths & Shared Config

# Container = "landing"
# Files were uploaded into landing/landing/... subfolder inside the container
# So full abfss path = abfss://landing@.../landing/<domain>/<table>/
LANDING = "abfss://landing@omniocdeveusst01.dfs.core.windows.net/landing"
CHKPT   = "abfss://checkpoint@omniocdeveusst01.dfs.core.windows.net/bronze"

# ── Autoloader options applied to every CSV source ───────────────────────────
CSV_OPTIONS = {
    # --- Format ---
    "cloudFiles.format"            : "csv",
    "header"                       : "true",        # first row is column names
    "sep"                          : ",",           # comma delimiter
    "quote"                        : '"',           # quote character
    "escape"                       : '"',           # escape inside quoted fields
    "multiLine"                    : "false",       # one JSON object per line

    # --- Nulls & whitespace ---
    "nullValue"                    : "",            # empty string → null
    "nanValue"                     : "NaN",
    "ignoreLeadingWhiteSpace"      : "true",
    "ignoreTrailingWhiteSpace"     : "true",

    # --- Schema handling ---
    "cloudFiles.inferColumnTypes"  : "true",        # infer int/double instead of all strings
    "cloudFiles.schemaEvolutionMode": "addNewColumns", # auto-add new cols from future files
    # cloudFiles.schemaLocation set per table inside load_csv()

    # --- Rescue ---
    "cloudFiles.rescuedDataColumn" : "_rescued_data", # store unparseable data here
}

print("✅ Config ready")
print(f"   LANDING : {LANDING}")
print(f"   CHKPT   : {CHKPT}")

# COMMAND ----------
from pyspark.sql.functions import current_timestamp, lit, input_file_name

def load_csv(source_path, target_table, extra_options=None):
    """
    Load CSV folder into a Delta bronze table using Autoloader (trigger once = batch mode).
    Adds metadata cols: _ingest_time, _source_file, _source_system.
    """
    table_name = target_table.split(".")[-1]
    schema_loc = f"{CHKPT}/schema/{table_name}"   # Autoloader stores inferred schema here
    chkpt_loc  = f"{CHKPT}/{table_name}"           # stream offset tracking

    options = {**CSV_OPTIONS, "cloudFiles.schemaLocation": schema_loc}
    if extra_options:
        options.update(extra_options)

    print(f"  → Reading  : {source_path}")
    print(f"  → Schema   : {schema_loc}")
    print(f"  → Checkpoint: {chkpt_loc}")

    df = (spark.readStream
        .format("cloudFiles")
        .options(**options)
        .load(source_path)
        .withColumn("_ingest_time",   current_timestamp())
        .withColumn("_source_file",   input_file_name())
        .withColumn("_source_system", lit("adls_csv"))
    )

    (df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", chkpt_loc)
        .option("mergeSchema",        "true")  # allow new columns from schema evolution
        .trigger(availableNow=True)            # process all pending files then stop
        .toTable(target_table)
        .awaitTermination()                    # block until complete
    )

    count = spark.table(target_table).count()
    print(f"  ✅ {target_table} → {count:,} rows\n")

# COMMAND ----------
# MAGIC %md ## 1 — suppliers_raw

# COMMAND ----------
load_csv(
    source_path  = f"{LANDING}/supply_chain/suppliers/",
    target_table = "omnioc_dev.bronze.suppliers_raw",
)

# COMMAND ----------
# MAGIC %md ## 2 — products_raw

# COMMAND ----------
load_csv(
    source_path  = f"{LANDING}/supply_chain/products/",
    target_table = "omnioc_dev.bronze.products_raw",
)

# COMMAND ----------
# MAGIC %md ## 3 — warehouses_raw

# COMMAND ----------
load_csv(
    source_path  = f"{LANDING}/supply_chain/warehouses/",
    target_table = "omnioc_dev.bronze.warehouses_raw",
)

# COMMAND ----------
# MAGIC %md ## 4 — exchange_rates_raw

# COMMAND ----------
load_csv(
    source_path  = f"{LANDING}/finance/exchange_rates/",
    target_table = "omnioc_dev.bronze.exchange_rates_raw",
)

# COMMAND ----------
# DBTITLE 1, Summary
print("\n" + "="*65)
print("  02 Bronze CSV — Load Summary")
print("="*65)
tables = {
    "suppliers_raw"     : 500,
    "products_raw"      : 5000,
    "warehouses_raw"    : 20,
    "exchange_rates_raw": 2190,   # 365 days x 6 currencies
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
