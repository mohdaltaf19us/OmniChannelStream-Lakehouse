# Databricks notebook source
# MAGIC %md # 04 — Bronze: Autoloader Parquet → Delta

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("catalog",         "omnioc_dev",       "Unity Catalog")
dbutils.widgets.text("storage_account", "omniocdeveusst01", "ADLS Account")
dbutils.widgets.text("trigger_once",    "true",             "trigger once?")

CATALOG         = dbutils.widgets.get("catalog")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
TRIGGER_ONCE    = dbutils.widgets.get("trigger_once").lower() == "true"

def adls(c, p=""): return f"abfss://{c}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{p}"
def trigger(): return {"once": True} if TRIGGER_ONCE else {"processingTime": "10 minutes"}

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")

# COMMAND ----------
# MAGIC %md ## 1 — shipments.parquet

source      = adls("landing",     "supply_chain/shipments.parquet")
checkpoint  = adls("checkpoint",  "autoloader/shipments/")
table       = f"{CATALOG}.bronze.shipments_raw"

q = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "parquet")
    .option("cloudFiles.schemaLocation",      checkpoint + "_schema")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("cloudFiles.includeExistingFiles","true")
    .option("mergeSchema",                    "true")
    .load(source)
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingest_time", F.current_timestamp())
    .withColumn("_source",      F.lit(source))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .option("mergeSchema", "true")
    .trigger(**trigger())
    .toTable(table)
)
q.awaitTermination()
print(f"✅ {table} | {spark.table(table).count():,} rows")
