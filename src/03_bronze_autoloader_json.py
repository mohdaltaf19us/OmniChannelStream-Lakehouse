# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Bronze: Autoloader JSON → Delta
# MAGIC Ingests: `customers.json` · `orders.json` · `order_lines.json` · `payments.json`
# MAGIC          `refunds.json` · `supplier_contracts.json` (nested struct flattening)
# MAGIC          `cdc_customers_changes.json` · `cdc_products_changes.json`

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, MapType

dbutils.widgets.text("catalog",         "omnioc_dev",       "Unity Catalog")
dbutils.widgets.text("storage_account", "omniocdeveusst01", "ADLS Account")
dbutils.widgets.text("trigger_once",    "true",             "trigger once?")

CATALOG         = dbutils.widgets.get("catalog")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
TRIGGER_ONCE    = dbutils.widgets.get("trigger_once").lower() == "true"

def adls(container, path=""):
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"

def trigger():
    return {"once": True} if TRIGGER_ONCE else {"processingTime": "10 minutes"}

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")

# COMMAND ----------
# MAGIC %md ## Shared helper

def autoloader_json(source_path, checkpoint_path, table,
                    multi_line=False, extra_transforms=None):
    opts = {
        "cloudFiles.format":              "json",
        "multiLine":                      str(multi_line).lower(),
        "inferColumnTypes":               "true",
        "cloudFiles.schemaLocation":      checkpoint_path + "_schema",
        "cloudFiles.schemaEvolutionMode": "rescue",
        "cloudFiles.includeExistingFiles":"true",
    }
    df = (
        spark.readStream
        .format("cloudFiles")
        .options(**opts)
        .load(source_path)
    )
    if extra_transforms:
        df = extra_transforms(df)
    q = (
        df
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingest_time", F.current_timestamp())
        .withColumn("_source",      F.lit(source_path))
        .writeStream
        .format("delta")
        .outputMode("append")
        .options(**{"checkpointLocation": checkpoint_path, "mergeSchema": "true"})
        .trigger(**trigger())
        .toTable(table)
    )
    q.awaitTermination()
    count = spark.table(table).count()
    print(f"✅ {table} | {count:,} rows")
    return count

# COMMAND ----------
# MAGIC %md ## 1 — customers.json (JSON Lines)

autoloader_json(
    source_path     = adls("landing", "finance/customers.json"),
    checkpoint_path = adls("checkpoint", "autoloader/customers/"),
    table           = f"{CATALOG}.bronze.customers_raw",
    multi_line      = False,
)

# COMMAND ----------
# MAGIC %md ## 2 — orders.json (JSON Lines)

autoloader_json(
    source_path     = adls("landing", "finance/orders.json"),
    checkpoint_path = adls("checkpoint", "autoloader/orders/"),
    table           = f"{CATALOG}.bronze.orders_raw",
    multi_line      = False,
)

# COMMAND ----------
# MAGIC %md ## 3 — order_lines.json

autoloader_json(
    source_path     = adls("landing", "finance/order_lines.json"),
    checkpoint_path = adls("checkpoint", "autoloader/order_lines/"),
    table           = f"{CATALOG}.bronze.order_lines_raw",
    multi_line      = False,
)

# COMMAND ----------
# MAGIC %md ## 4 — payments.json

autoloader_json(
    source_path     = adls("landing", "finance/payments.json"),
    checkpoint_path = adls("checkpoint", "autoloader/payments/"),
    table           = f"{CATALOG}.bronze.payments_raw",
    multi_line      = False,
)

# COMMAND ----------
# MAGIC %md ## 5 — refunds.json

autoloader_json(
    source_path     = adls("landing", "finance/refunds.json"),
    checkpoint_path = adls("checkpoint", "autoloader/refunds/"),
    table           = f"{CATALOG}.bronze.refunds_raw",
    multi_line      = False,
)

# COMMAND ----------
# MAGIC %md ## 6 — supplier_contracts.json (nested sla_terms struct → flatten)

def flatten_contracts(df):
    """Flatten nested sla_terms struct into individual columns."""
    return (
        df
        .withColumn("sla_lead_time_days",              F.col("sla_terms.lead_time_days"))
        .withColumn("sla_defect_rate_max_pct",         F.col("sla_terms.defect_rate_max_pct"))
        .withColumn("sla_on_time_target_pct",          F.col("sla_terms.on_time_delivery_target_pct"))
        .withColumn("sla_penalty_per_day_late_usd",    F.col("sla_terms.penalty_per_day_late_usd"))
        .drop("sla_terms")
    )

autoloader_json(
    source_path      = adls("landing", "supply_chain/supplier_contracts.json"),
    checkpoint_path  = adls("checkpoint", "autoloader/supplier_contracts/"),
    table            = f"{CATALOG}.bronze.supplier_contracts_raw",
    multi_line       = True,
    extra_transforms = flatten_contracts,
)

# COMMAND ----------
# MAGIC %md ## 7 — CDC change records

# CDC customers
autoloader_json(
    source_path     = adls("landing", "cdc/cdc_customers_changes.json"),
    checkpoint_path = adls("checkpoint", "autoloader/cdc_customers/"),
    table           = f"{CATALOG}.bronze.cdc_customers_raw",
    multi_line      = False,
)

# CDC products
autoloader_json(
    source_path     = adls("landing", "cdc/cdc_products_changes.json"),
    checkpoint_path = adls("checkpoint", "autoloader/cdc_products/"),
    table           = f"{CATALOG}.bronze.cdc_products_raw",
    multi_line      = False,
)

# COMMAND ----------
# MAGIC %md ## 8 — Bad data / poison pills (DQ testing)

autoloader_json(
    source_path     = adls("landing", "bad_data/poison_pills_orders.json"),
    checkpoint_path = adls("checkpoint", "autoloader/poison_pills/"),
    table           = f"{CATALOG}.bronze.poison_pills_raw",
    multi_line      = False,
)

# COMMAND ----------
# MAGIC %md ## Summary

tables = [
    "customers_raw", "orders_raw", "order_lines_raw", "payments_raw",
    "refunds_raw", "supplier_contracts_raw",
    "cdc_customers_raw", "cdc_products_raw", "poison_pills_raw",
]
print(f"\n{'Table':<50} {'Rows':>10}")
print("─" * 63)
for t in tables:
    full = f"{CATALOG}.bronze.{t}"
    try:
        r = spark.table(full).count()
        print(f"{full:<50} {r:>10,}")
    except Exception as e:
        print(f"{full:<50} ❌ {e}")
