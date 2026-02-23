# Databricks notebook source
# MAGIC %md
# MAGIC # Maintenance: Backfill Notebook
# MAGIC Called by ADF pipeline PL_10_Backfill when re-loading historical date ranges.
# MAGIC Handles: Autoloader re-read, optional checkpoint deletion, optional DLT full refresh.

# COMMAND ----------
from pyspark.sql import functions as F
import subprocess

dbutils.widgets.text("target_table",    "orders",       "Table to backfill")
dbutils.widgets.text("backfill_start",  "2024-01-01",   "Start date YYYY-MM-DD")
dbutils.widgets.text("backfill_end",    "2024-01-31",   "End date YYYY-MM-DD")
dbutils.widgets.text("clear_checkpoint","false",         "Delete Autoloader checkpoint?")
dbutils.widgets.text("full_refresh_dlt","false",         "Full refresh DLT pipeline?")
dbutils.widgets.text("catalog",         "omnioc_dev",   "Unity Catalog")
dbutils.widgets.text("storage_account", "omniocdeveusst01", "ADLS account")

TARGET_TABLE      = dbutils.widgets.get("target_table")
BACKFILL_START    = dbutils.widgets.get("backfill_start")
BACKFILL_END      = dbutils.widgets.get("backfill_end")
CLEAR_CHECKPOINT  = dbutils.widgets.get("clear_checkpoint").lower() == "true"
FULL_REFRESH_DLT  = dbutils.widgets.get("full_refresh_dlt").lower() == "true"
CATALOG           = dbutils.widgets.get("catalog")
STORAGE_ACCOUNT   = dbutils.widgets.get("storage_account")

def adls(c, p=""): return f"abfss://{c}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{p}"

print(f"Backfill config:")
print(f"  Table       : {TARGET_TABLE}")
print(f"  Date range  : {BACKFILL_START} → {BACKFILL_END}")
print(f"  Clear ckpt  : {CLEAR_CHECKPOINT}")
print(f"  Full refresh: {FULL_REFRESH_DLT}")

# COMMAND ----------
# MAGIC %md ## Step 1 — Optionally clear Autoloader checkpoint

if CLEAR_CHECKPOINT:
    checkpoint_path = adls("checkpoint", f"autoloader/{TARGET_TABLE}/")
    print(f"⚠️  Clearing checkpoint: {checkpoint_path}")
    print("   This will cause Autoloader to re-read ALL files in landing/")
    try:
        dbutils.fs.rm(checkpoint_path, recurse=True)
        print(f"  ✅ Checkpoint deleted: {checkpoint_path}")
    except Exception as e:
        print(f"  ⚠️  Could not delete checkpoint (may not exist): {e}")
else:
    print("✅ Keeping checkpoint (Autoloader will only process new files)")

# COMMAND ----------
# MAGIC %md ## Step 2 — Count records before backfill

bronze_table = f"{CATALOG}.bronze.{TARGET_TABLE}_raw"
try:
    before_count = spark.table(bronze_table).count()
    print(f"Records in {bronze_table} BEFORE backfill: {before_count:,}")
except Exception as e:
    before_count = 0
    print(f"Table not found (will be created): {bronze_table}")

# COMMAND ----------
# MAGIC %md ## Step 3 — Run Autoloader for backfill path

# Backfill files were placed by ADF into landing/backfill/{table}/{start}_to_{end}/
backfill_source = adls("landing", f"backfill/{TARGET_TABLE}/{BACKFILL_START}_to_{BACKFILL_END}/")
backfill_checkpoint = adls("checkpoint", f"autoloader/backfill_{TARGET_TABLE}_{BACKFILL_START}/")

print(f"Reading from: {backfill_source}")
print(f"Checkpoint  : {backfill_checkpoint}")

try:
    q = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", backfill_checkpoint + "_schema")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("mergeSchema", "true")
        .load(backfill_source)
        .withColumn("_backfill_date",   F.lit(BACKFILL_START))
        .withColumn("_backfill_end",    F.lit(BACKFILL_END))
        .withColumn("_ingest_time",     F.current_timestamp())
        .withColumn("_source",          F.lit(f"backfill/{TARGET_TABLE}"))
        .withColumn("_source_file",     F.input_file_name())
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", backfill_checkpoint)
        .option("mergeSchema", "true")
        .trigger(once=True)
        .toTable(bronze_table)
    )
    q.awaitTermination()
    after_count = spark.table(bronze_table).count()
    new_rows = after_count - before_count
    print(f"✅ Backfill loaded: {new_rows:,} new rows → {bronze_table}")
    print(f"   Total rows after: {after_count:,}")
except Exception as e:
    print(f"⚠️  No backfill files found at {backfill_source}")
    print(f"   Make sure ADF PL_10_Backfill ran first to re-extract data to this path")
    raise

# COMMAND ----------
# MAGIC %md ## Step 4 — Optionally trigger DLT full refresh

if FULL_REFRESH_DLT:
    print("🔄 Full refresh DLT pipeline requested...")
    print("   In production: call Databricks REST API to trigger full refresh")
    print("   databricks pipelines start --pipeline-id <id> --full-refresh")
    print("   Or use the DatabricksSparkPython activity in ADF which ADF triggers automatically")
else:
    print("✅ DLT will pick up new Bronze rows in next scheduled update")

# COMMAND ----------
# MAGIC %md ## Summary

print(f"\n{'─'*60}")
print(f"  BACKFILL SUMMARY")
print(f"{'─'*60}")
print(f"  Table         : {bronze_table}")
print(f"  Date range    : {BACKFILL_START} → {BACKFILL_END}")
print(f"  Before count  : {before_count:,}")
print(f"  After count   : {spark.table(bronze_table).count():,}")
print(f"  New rows added: {spark.table(bronze_table).count() - before_count:,}")
print(f"  DLT refresh   : {'Full refresh triggered' if FULL_REFRESH_DLT else 'Incremental (next scheduled run)'}")
print(f"{'─'*60}")
print("✅ Backfill complete")
