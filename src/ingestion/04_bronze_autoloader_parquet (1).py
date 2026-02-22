# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Bronze Autoloader — Parquet Files
# MAGIC Container: `landing` | Subfolder: `landing/` | Format: Parquet | Trigger: availableNow

# COMMAND ----------
# DBTITLE 1, Config
from pyspark.sql.functions import current_timestamp, lit, input_file_name, max as spark_max

LANDING = "abfss://landing@omniocdeveusst01.dfs.core.windows.net/landing"
CHKPT   = "abfss://checkpoint@omniocdeveusst01.dfs.core.windows.net/bronze"

print(f"✅ LANDING : {LANDING}")
print(f"✅ CHKPT   : {CHKPT}")

# COMMAND ----------
# MAGIC %md ## 1 — shipments_raw

# COMMAND ----------
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "parquet")
    .option("cloudFiles.schemaLocation",      f"{CHKPT}/schema/shipments_raw")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") # add new cols from future files
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data") # schema mismatch rows land here
    # Note: parquet is self-describing so inferColumnTypes not needed
    # Note: multiLine not applicable to parquet
    .load(f"{LANDING}/supply_chain/shipments/")
    .withColumn("_ingest_time",   current_timestamp())
    .withColumn("_source_file",   input_file_name())
    .withColumn("_source_system", lit("adls_parquet"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHKPT}/shipments_raw")
    .option("mergeSchema",        "true")
    .trigger(availableNow=True)
    .toTable("omnioc_dev.bronze.shipments_raw")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md ## Summary — Live from Delta Table

# COMMAND ----------
# DBTITLE 1, Row count and schema from table
df    = spark.table("omnioc_dev.bronze.shipments_raw")
rows  = df.count()
cols  = len(df.columns)

print("="*65)
print("  04 Bronze Parquet — Live Summary")
print("="*65)
print(f"  Table   : omnioc_dev.bronze.shipments_raw")
print(f"  Rows    : {rows:,}  {'✅' if rows > 0 else '❌ EMPTY'}")
print(f"  Columns : {cols}")
print("="*65)

# COMMAND ----------
# DBTITLE 1, Latest ingest time
latest = df.select(spark_max("_ingest_time")).collect()[0][0]
print(f"  Latest _ingest_time : {latest}")

# COMMAND ----------
# DBTITLE 1, Source files loaded
print("  Source files loaded:")
df.select("_source_file").distinct().show(truncate=False)

# COMMAND ----------
# DBTITLE 1, Shipment status breakdown — from actual table data
print("  Shipment status breakdown:")
df.groupBy("status").count().orderBy("count", ascending=False).show()

# COMMAND ----------
# DBTITLE 1, Carrier breakdown
print("  Carrier breakdown:")
df.groupBy("carrier").count().orderBy("count", ascending=False).show()

# COMMAND ----------
# DBTITLE 1, Sample rows
print("  Sample rows:")
df.select(
    "shipment_id","supplier_id","destination_warehouse_id",
    "product_id","quantity","status","ship_date","_ingest_time"
).show(10, truncate=False)

# COMMAND ----------
# DBTITLE 1, Rescued data check
rescued = df.filter("_rescued_data is not null").count()
print(f"  Rescued rows (schema mismatch) : {rescued}")
if rescued > 0:
    print("  Sample rescued:")
    df.filter("_rescued_data is not null").select("_rescued_data").show(5, truncate=False)
