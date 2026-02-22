# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze: Autoloader CSV → Delta
# MAGIC Ingests: `suppliers.csv` · `products.csv` · `warehouses.csv` · `exchange_rates.csv`
# MAGIC Pattern: `cloudFiles` · `trigger(once=True)` · Schema rescue · Audit columns

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("catalog",         "omnioc_dev",       "Unity Catalog")
dbutils.widgets.text("storage_account", "omniocdeveusst01", "ADLS Account")
dbutils.widgets.text("trigger_once",    "true",             "Run once (true) or continuous")

CATALOG         = dbutils.widgets.get("catalog")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
TRIGGER_ONCE    = dbutils.widgets.get("trigger_once").lower() == "true"

def adls(container, path=""):
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"

def trigger():
    return {"once": True} if TRIGGER_ONCE else {"processingTime": "5 minutes"}

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
print(f"✅ Config | Catalog: {CATALOG} | Trigger once: {TRIGGER_ONCE}")

# COMMAND ----------
# MAGIC %md ## Shared Autoloader helper

def autoloader_csv(source_path, checkpoint_path, table, extra_opts=None):
    """Generic Autoloader CSV ingestion — returns awaitTermination'd query."""
    opts = {
        "cloudFiles.format":             "csv",
        "header":                        "true",
        "inferColumnTypes":              "true",
        "cloudFiles.schemaLocation":     checkpoint_path + "_schema",
        "cloudFiles.schemaEvolutionMode":"rescue",
        "cloudFiles.includeExistingFiles":"true",
        "cloudFiles.useIncrementalListing":"true",
    }
    if extra_opts:
        opts.update(extra_opts)

    q = (
        spark.readStream
        .format("cloudFiles")
        .options(**opts)
        .load(source_path)
        .withColumn("_source_file",  F.input_file_name())
        .withColumn("_ingest_time",  F.current_timestamp())
        .withColumn("_source",       F.lit(source_path))
        .withColumn("_ingest_date",  F.to_date(F.current_timestamp()))
        .writeStream
        .format("delta")
        .outputMode("append")
        .options(**{
            "checkpointLocation": checkpoint_path,
            "mergeSchema":        "true",
        })
        .trigger(**trigger())
        .toTable(table)
    )
    q.awaitTermination()
    count = spark.table(table).count()
    print(f"✅ {table} | {count:,} rows")
    return count

# COMMAND ----------
# MAGIC %md ## 1 — suppliers.csv

autoloader_csv(
    source_path      = adls("landing", "supply_chain/suppliers.csv"),
    checkpoint_path  = adls("checkpoint", "autoloader/suppliers/"),
    table            = f"{CATALOG}.bronze.suppliers_raw",
)

# COMMAND ----------
# MAGIC %md ## 2 — products.csv

autoloader_csv(
    source_path      = adls("landing", "supply_chain/products.csv"),
    checkpoint_path  = adls("checkpoint", "autoloader/products/"),
    table            = f"{CATALOG}.bronze.products_raw",
)

# COMMAND ----------
# MAGIC %md ## 3 — warehouses.csv

autoloader_csv(
    source_path      = adls("landing", "supply_chain/warehouses.csv"),
    checkpoint_path  = adls("checkpoint", "autoloader/warehouses/"),
    table            = f"{CATALOG}.bronze.warehouses_raw",
)

# COMMAND ----------
# MAGIC %md ## 4 — exchange_rates.csv (Finance)

autoloader_csv(
    source_path      = adls("landing", "finance/exchange_rates.csv"),
    checkpoint_path  = adls("checkpoint", "autoloader/exchange_rates/"),
    table            = f"{CATALOG}.bronze.exchange_rates_raw",
    extra_opts       = {"dateFormat": "yyyy-MM-dd"},
)

# COMMAND ----------
# MAGIC %md ## 5 — Summary

tables = [
    f"{CATALOG}.bronze.suppliers_raw",
    f"{CATALOG}.bronze.products_raw",
    f"{CATALOG}.bronze.warehouses_raw",
    f"{CATALOG}.bronze.exchange_rates_raw",
]
print(f"\n{'Table':<45} {'Rows':>10}")
print("─" * 58)
for t in tables:
    try:
        r = spark.table(t).count()
        print(f"{t:<45} {r:>10,}")
    except Exception as e:
        print(f"{t:<45} ERROR: {e}")
