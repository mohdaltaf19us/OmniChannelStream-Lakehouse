# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Bronze Autoloader — Parquet Files
# MAGIC
# MAGIC **Actual ADLS path (container=landing, subfolder=landing/):**
# MAGIC - `landing/landing/supply_chain/shipments/shipments.parquet`
# MAGIC
# MAGIC **Writes to:** `omnioc_dev.bronze.shipments_raw` via Autoloader trigger-once (batch mode)

# COMMAND ----------
# DBTITLE 1, Paths & Config

# Container = "landing"
# Files are inside the landing/landing/... subfolder
LANDING = "abfss://landing@omniocdeveusst01.dfs.core.windows.net/landing"
CHKPT   = "abfss://checkpoint@omniocdeveusst01.dfs.core.windows.net/bronze"

# ── Autoloader options for Parquet ────────────────────────────────────────────
PARQUET_OPTIONS = {
    # --- Format ---
    "cloudFiles.format"             : "parquet",

    # --- Schema handling ---
    # Parquet is self-describing so schema inference is exact (not estimated like CSV)
    # schemaLocation still needed for Autoloader to track schema across runs
    "cloudFiles.schemaEvolutionMode": "addNewColumns",  # add new cols from future files

    # --- Rescue ---
    # Parquet supports rescue col for schema mismatches
    "cloudFiles.rescuedDataColumn"  : "_rescued_data",
}

SOURCE_PATH  = f"{LANDING}/supply_chain/shipments/"
TARGET_TABLE = "omnioc_dev.bronze.shipments_raw"
SCHEMA_LOC   = f"{CHKPT}/schema/shipments_raw"
CHKPT_LOC    = f"{CHKPT}/shipments_raw"

print("✅ Config ready")
print(f"   Source      : {SOURCE_PATH}")
print(f"   Target      : {TARGET_TABLE}")
print(f"   Schema loc  : {SCHEMA_LOC}")
print(f"   Checkpoint  : {CHKPT_LOC}")

# COMMAND ----------
from pyspark.sql.functions import current_timestamp, lit, input_file_name

# COMMAND ----------
# DBTITLE 1, Read Stream
options = {**PARQUET_OPTIONS, "cloudFiles.schemaLocation": SCHEMA_LOC}

df = (spark.readStream
    .format("cloudFiles")
    .options(**options)
    .load(SOURCE_PATH)
    .withColumn("_ingest_time",   current_timestamp())  # when Databricks ingested this row
    .withColumn("_source_file",   input_file_name())    # full path of source parquet file
    .withColumn("_source_system", lit("adls_parquet"))  # tag for lineage tracking
)

print("✅ Stream reader connected")
print(f"   Schema will be inferred from parquet file metadata")
df.printSchema()

# COMMAND ----------
# DBTITLE 1, Write to Delta
print(f"Writing to {TARGET_TABLE}...")

(df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHKPT_LOC)
    .option("mergeSchema",        "true")  # merge parquet schema into existing Delta schema
    .trigger(availableNow=True)            # batch mode: process all pending files then stop
    .toTable(TARGET_TABLE)
    .awaitTermination()
)

count = spark.table(TARGET_TABLE).count()
print(f"\n✅ {TARGET_TABLE} → {count:,} rows loaded")

# COMMAND ----------
# DBTITLE 1, Preview & Verify
print("\n" + "="*65)
print("  04 Bronze Parquet — Verification")
print("="*65)

df_check = spark.table(TARGET_TABLE)
count    = df_check.count()

print(f"  Rows loaded    : {count:,}  (expected ≥ 30,000)")
print(f"  Columns        : {len(df_check.columns)}")
print(f"  Source tag     : adls_parquet")
print(f"  Target table   : {TARGET_TABLE}")

status = "✅ PASS" if count >= 30000 else ("⚠️ LOW" if count > 0 else "❌ EMPTY")
print(f"  Status         : {status}")
print("="*65)

# Show sample rows
print("\nSample rows:")
df_check.select(
    "shipment_id","supplier_id","destination_warehouse_id",
    "product_id","quantity","status","ship_date","_ingest_time"
).show(5, truncate=False)

# Status distribution
print("Status breakdown:")
df_check.groupBy("status").count().orderBy("count", ascending=False).show()
