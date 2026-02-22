# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Bronze Autoloader — CSV Files
# MAGIC Container: `landing` | Subfolder: `landing/` | Format: CSV | Trigger: availableNow

# COMMAND ----------
# DBTITLE 1, Config
from pyspark.sql.functions import current_timestamp, lit, input_file_name, max as spark_max

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
    .option("cloudFiles.inferColumnTypes",    "true")          # int/double instead of all strings
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") # auto-add cols from future files
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data") # store unparseable fields here
    .option("header",                         "true")
    .option("sep",                            ",")
    .option("nullValue",                      "")
    .option("ignoreLeadingWhiteSpace",        "true")
    .option("ignoreTrailingWhiteSpace",       "true")
    .load(f"{LANDING}/supply_chain/suppliers/")
    .withColumn("_ingest_time",   current_timestamp())
    .withColumn("_source_file",   input_file_name())
    .withColumn("_source_system", lit("adls_csv"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHKPT}/suppliers_raw")
    .option("mergeSchema",        "true")
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
    .option("cloudFiles.inferColumnTypes",    "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("header",                         "true")
    .option("sep",                            ",")
    .option("nullValue",                      "")
    .option("ignoreLeadingWhiteSpace",        "true")
    .option("ignoreTrailingWhiteSpace",       "true")
    .load(f"{LANDING}/supply_chain/products/")
    .withColumn("_ingest_time",   current_timestamp())
    .withColumn("_source_file",   input_file_name())
    .withColumn("_source_system", lit("adls_csv"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHKPT}/products_raw")
    .option("mergeSchema",        "true")
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
    .option("cloudFiles.inferColumnTypes",    "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("header",                         "true")
    .option("sep",                            ",")
    .option("nullValue",                      "")
    .option("ignoreLeadingWhiteSpace",        "true")
    .option("ignoreTrailingWhiteSpace",       "true")
    .load(f"{LANDING}/supply_chain/warehouses/")
    .withColumn("_ingest_time",   current_timestamp())
    .withColumn("_source_file",   input_file_name())
    .withColumn("_source_system", lit("adls_csv"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHKPT}/warehouses_raw")
    .option("mergeSchema",        "true")
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
    .option("cloudFiles.inferColumnTypes",    "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("header",                         "true")
    .option("sep",                            ",")
    .option("nullValue",                      "")
    .option("ignoreLeadingWhiteSpace",        "true")
    .option("ignoreTrailingWhiteSpace",       "true")
    .load(f"{LANDING}/finance/exchange_rates/")
    .withColumn("_ingest_time",   current_timestamp())
    .withColumn("_source_file",   input_file_name())
    .withColumn("_source_system", lit("adls_csv"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHKPT}/exchange_rates_raw")
    .option("mergeSchema",        "true")
    .trigger(availableNow=True)
    .toTable("omnioc_dev.bronze.exchange_rates_raw")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md ## Summary — Live from Delta Tables

# COMMAND ----------
# DBTITLE 1, Row counts from tables
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
print("  Latest _ingest_time:")
print("-"*65)
for tbl in ["suppliers_raw","products_raw","warehouses_raw","exchange_rates_raw"]:
    latest = (spark.table(f"omnioc_dev.bronze.{tbl}")
              .select(spark_max("_ingest_time")).collect()[0][0])
    print(f"    {tbl:<33} {str(latest)}")
print("="*65)

# COMMAND ----------
# DBTITLE 1, Source files seen by Autoloader
print("  Source files loaded (_source_file):")
print("-"*65)
for tbl in ["suppliers_raw","products_raw","warehouses_raw","exchange_rates_raw"]:
    files = (spark.table(f"omnioc_dev.bronze.{tbl}")
             .select("_source_file").distinct().collect())
    for f in files:
        print(f"    {tbl:<33} {f[0].split('/')[-1]}")
print("="*65)

# COMMAND ----------
# DBTITLE 1, Sample rows — suppliers
spark.table("omnioc_dev.bronze.suppliers_raw").select(
    "supplier_id","supplier_name","country","rating","is_active","_ingest_time"
).show(5, truncate=False)

# COMMAND ----------
# DBTITLE 1, Sample rows — products
spark.table("omnioc_dev.bronze.products_raw").select(
    "product_id","product_name","category","unit_cost","unit_price","supplier_id"
).show(5, truncate=False)

# COMMAND ----------
# DBTITLE 1, Sample rows — exchange_rates
spark.table("omnioc_dev.bronze.exchange_rates_raw").select(
    "rate_date","base_currency","target_currency","exchange_rate"
).show(10, truncate=False)
