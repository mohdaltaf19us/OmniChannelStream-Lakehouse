# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Bronze Autoloader — CSV Files
# MAGIC Container: `landing` | Subfolder: `landing/` | inferColumnTypes: **false** + explicit casts

# COMMAND ----------
# DBTITLE 1, Config
from pyspark.sql.functions import col, current_timestamp, lit, input_file_name, max as spark_max
from pyspark.sql.types import *

LANDING = "abfss://landing@omniocdeveusst01.dfs.core.windows.net/landing"
CHKPT   = "abfss://checkpoint@omniocdeveusst01.dfs.core.windows.net/bronze"

print(f"✅ LANDING : {LANDING}")
print(f"✅ CHKPT   : {CHKPT}")

# COMMAND ----------
# MAGIC %md ## 1 — suppliers_raw

# COMMAND ----------
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "csv")
    .option("cloudFiles.schemaLocation",      f"{CHKPT}/schema/suppliers_raw")
    .option("cloudFiles.inferColumnTypes",    "false")         # read all as STRING first
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("header",                         "true")
    .option("sep",                            ",")
    .option("nullValue",                      "")
    .option("ignoreLeadingWhiteSpace",        "true")
    .option("ignoreTrailingWhiteSpace",       "true")
    .load(f"{LANDING}/supply_chain/suppliers/")
    # ── Type casts — every non-string column cast explicitly ────────────────
    .withColumn("rating",              col("rating").cast(DoubleType()))
    .withColumn("lead_time_days",      col("lead_time_days").cast(IntegerType()))
    .withColumn("payment_terms_days",  col("payment_terms_days").cast(IntegerType()))
    .withColumn("is_active",           col("is_active").cast(BooleanType()))
    # ── Metadata ────────────────────────────────────────────────────────────
    .withColumn("_ingest_time",        current_timestamp())
    .withColumn("_source_file",        input_file_name())
    .withColumn("_source_system",      lit("adls_csv"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",      f"{CHKPT}/suppliers_raw")
    .option("mergeSchema",             "true")
    .trigger(availableNow=True)
    .toTable("omnioc_dev.bronze.suppliers_raw")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md ## 2 — products_raw

# COMMAND ----------
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "csv")
    .option("cloudFiles.schemaLocation",      f"{CHKPT}/schema/products_raw")
    .option("cloudFiles.inferColumnTypes",    "false")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("header",                         "true")
    .option("sep",                            ",")
    .option("nullValue",                      "")
    .option("ignoreLeadingWhiteSpace",        "true")
    .option("ignoreTrailingWhiteSpace",       "true")
    .load(f"{LANDING}/supply_chain/products/")
    # ── Type casts ──────────────────────────────────────────────────────────
    .withColumn("unit_cost",           col("unit_cost").cast(DoubleType()))
    .withColumn("unit_price",          col("unit_price").cast(DoubleType()))
    .withColumn("weight_kg",           col("weight_kg").cast(DoubleType()))
    .withColumn("is_active",           col("is_active").cast(BooleanType()))
    .withColumn("reorder_point",       col("reorder_point").cast(IntegerType()))
    .withColumn("reorder_quantity",    col("reorder_quantity").cast(IntegerType()))
    # ── Metadata ────────────────────────────────────────────────────────────
    .withColumn("_ingest_time",        current_timestamp())
    .withColumn("_source_file",        input_file_name())
    .withColumn("_source_system",      lit("adls_csv"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",      f"{CHKPT}/products_raw")
    .option("mergeSchema",             "true")
    .trigger(availableNow=True)
    .toTable("omnioc_dev.bronze.products_raw")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md ## 3 — warehouses_raw

# COMMAND ----------
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "csv")
    .option("cloudFiles.schemaLocation",      f"{CHKPT}/schema/warehouses_raw")
    .option("cloudFiles.inferColumnTypes",    "false")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("header",                         "true")
    .option("sep",                            ",")
    .option("nullValue",                      "")
    .option("ignoreLeadingWhiteSpace",        "true")
    .option("ignoreTrailingWhiteSpace",       "true")
    .load(f"{LANDING}/supply_chain/warehouses/")
    # ── Type casts ──────────────────────────────────────────────────────────
    .withColumn("capacity_sqft",       col("capacity_sqft").cast(IntegerType()))
    .withColumn("max_sku_capacity",    col("max_sku_capacity").cast(IntegerType()))
    .withColumn("is_active",           col("is_active").cast(BooleanType()))
    # ── Metadata ────────────────────────────────────────────────────────────
    .withColumn("_ingest_time",        current_timestamp())
    .withColumn("_source_file",        input_file_name())
    .withColumn("_source_system",      lit("adls_csv"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",      f"{CHKPT}/warehouses_raw")
    .option("mergeSchema",             "true")
    .trigger(availableNow=True)
    .toTable("omnioc_dev.bronze.warehouses_raw")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md ## 4 — exchange_rates_raw

# COMMAND ----------
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "csv")
    .option("cloudFiles.schemaLocation",      f"{CHKPT}/schema/exchange_rates_raw")
    .option("cloudFiles.inferColumnTypes",    "false")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("header",                         "true")
    .option("sep",                            ",")
    .option("nullValue",                      "")
    .option("ignoreLeadingWhiteSpace",        "true")
    .option("ignoreTrailingWhiteSpace",       "true")
    .load(f"{LANDING}/finance/exchange_rates/")
    # ── Type casts ──────────────────────────────────────────────────────────
    .withColumn("exchange_rate",       col("exchange_rate").cast(DoubleType()))
    # ── Metadata ────────────────────────────────────────────────────────────
    .withColumn("_ingest_time",        current_timestamp())
    .withColumn("_source_file",        input_file_name())
    .withColumn("_source_system",      lit("adls_csv"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",      f"{CHKPT}/exchange_rates_raw")
    .option("mergeSchema",             "true")
    .trigger(availableNow=True)
    .toTable("omnioc_dev.bronze.exchange_rates_raw")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md ## Summary — Live from Delta Tables

# COMMAND ----------
# DBTITLE 1, Row counts + column counts
print("="*65)
print("  02 Bronze CSV — Live Summary")
print("="*65)
print(f"  {'Table':<33} {'Rows':>10}  {'Cols':>5}  Status")
print("-"*65)
for tbl in ["suppliers_raw","products_raw","warehouses_raw","exchange_rates_raw"]:
    df     = spark.table(f"omnioc_dev.bronze.{tbl}")
    rows   = df.count()
    cols   = len(df.columns)
    status = "✅" if rows > 0 else "❌ EMPTY"
    print(f"  {status}  {tbl:<31} {rows:>10,}  {cols:>5}")
print("="*65)

# COMMAND ----------
# DBTITLE 1, Latest ingest time per table
for tbl in ["suppliers_raw","products_raw","warehouses_raw","exchange_rates_raw"]:
    latest = (spark.table(f"omnioc_dev.bronze.{tbl}")
              .select(spark_max("_ingest_time")).collect()[0][0])
    print(f"  {tbl:<33}  last ingest: {latest}")

# COMMAND ----------
# DBTITLE 1, Schema verification — confirm types are correct
print("  suppliers_raw schema (verify rating=double, is_active=boolean):")
spark.table("omnioc_dev.bronze.suppliers_raw").select(
    "supplier_id","supplier_name","country","rating","lead_time_days","is_active"
).printSchema()

# COMMAND ----------
# DBTITLE 1, Sample data
spark.table("omnioc_dev.bronze.suppliers_raw").select(
    "supplier_id","supplier_name","country","rating","is_active","_ingest_time"
).show(5, truncate=False)

spark.table("omnioc_dev.bronze.exchange_rates_raw").select(
    "rate_date","base_currency","target_currency","exchange_rate"
).show(5, truncate=False)
