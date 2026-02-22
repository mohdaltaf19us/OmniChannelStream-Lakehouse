# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Bronze Autoloader — Parquet Files
# MAGIC Container: `landing` | Subfolder: `landing/` | Format: Parquet | Trigger: availableNow
# MAGIC
# MAGIC Parquet is self-describing so types come from file metadata, not inference.
# MAGIC Explicit casts added as a safeguard to guarantee schema matches DDL exactly.

# COMMAND ----------
# DBTITLE 1, Config
from pyspark.sql.functions import col, current_timestamp, lit, input_file_name, max as spark_max
from pyspark.sql.types import *

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
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    # inferColumnTypes not applicable to parquet — types come from file metadata
    .load(f"{LANDING}/supply_chain/shipments/")
    # ── Type casts — enforce DDL schema regardless of what parquet metadata says ──
    .withColumn("quantity",                col("quantity").cast(IntegerType()))
    .withColumn("unit_cost",               col("unit_cost").cast(DoubleType()))
    .withColumn("total_cost",              col("total_cost").cast(DoubleType()))
    # ── Metadata ────────────────────────────────────────────────────────────
    .withColumn("_ingest_time",            current_timestamp())
    .withColumn("_source_file",            input_file_name())
    .withColumn("_source_system",          lit("adls_parquet"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",          f"{CHKPT}/shipments_raw")
    .option("mergeSchema",                 "true")
    .trigger(availableNow=True)
    .toTable("omnioc_dev.bronze.shipments_raw")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md ## Summary — Live from Delta Table

# COMMAND ----------
# DBTITLE 1, Row count and schema
df   = spark.table("omnioc_dev.bronze.shipments_raw")
rows = df.count()
cols = len(df.columns)

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
df.select("_source_file").distinct().show(truncate=False)

# COMMAND ----------
# DBTITLE 1, Shipment status breakdown
df.groupBy("status").count().orderBy("count", ascending=False).show()

# COMMAND ----------
# DBTITLE 1, Carrier breakdown
df.groupBy("carrier").count().orderBy("count", ascending=False).show()

# COMMAND ----------
# DBTITLE 1, Rescued rows check
rescued = df.filter("_rescued_data is not null").count()
print(f"  Rescued rows : {rescued}")
if rescued > 0:
    df.filter("_rescued_data is not null").select("_rescued_data").show(5, truncate=False)

# COMMAND ----------
# DBTITLE 1, Sample rows
df.select(
    "shipment_id","supplier_id","destination_warehouse_id",
    "product_id","quantity","status","ship_date","_ingest_time"
).show(10, truncate=False)
