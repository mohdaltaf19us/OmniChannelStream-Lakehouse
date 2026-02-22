# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Bronze Autoloader — JSON Files
# MAGIC Container: `landing` | Subfolder: `landing/` | Format: JSON | Trigger: availableNow

# COMMAND ----------
# DBTITLE 1, Config
from pyspark.sql.functions import current_timestamp, lit, input_file_name, max as spark_max

LANDING = "abfss://landing@omniocdeveusst01.dfs.core.windows.net/landing"
CHKPT   = "abfss://checkpoint@omniocdeveusst01.dfs.core.windows.net/bronze"

print(f"✅ LANDING : {LANDING}")
print(f"✅ CHKPT   : {CHKPT}")

# COMMAND ----------
# MAGIC %md ## 1 — customers_raw

# COMMAND ----------
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "json")
    .option("cloudFiles.schemaLocation",      f"{CHKPT}/schema/customers_raw")
    .option("cloudFiles.inferColumnTypes",    "true")          # infer boolean/int/double
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") # auto-add new fields
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data") # unparseable fields land here
    .option("multiLine",                      "false")         # one JSON object per line
    .option("mode",                           "PERMISSIVE")    # don't fail on bad rows
    .load(f"{LANDING}/finance/customers/")
    .withColumn("_ingest_time",   current_timestamp())
    .withColumn("_source_file",   input_file_name())
    .withColumn("_source_system", lit("adls_json"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHKPT}/customers_raw")
    .option("mergeSchema",        "true")
    .trigger(availableNow=True)
    .toTable("omnioc_dev.bronze.customers_raw")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md ## 2 — orders_raw

# COMMAND ----------
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "json")
    .option("cloudFiles.schemaLocation",      f"{CHKPT}/schema/orders_raw")
    .option("cloudFiles.inferColumnTypes",    "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("multiLine",                      "false")
    .option("mode",                           "PERMISSIVE")
    .load(f"{LANDING}/finance/orders/")
    .withColumn("_ingest_time",   current_timestamp())
    .withColumn("_source_file",   input_file_name())
    .withColumn("_source_system", lit("adls_json"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHKPT}/orders_raw")
    .option("mergeSchema",        "true")
    .trigger(availableNow=True)
    .toTable("omnioc_dev.bronze.orders_raw")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md ## 3 — order_lines_raw

# COMMAND ----------
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "json")
    .option("cloudFiles.schemaLocation",      f"{CHKPT}/schema/order_lines_raw")
    .option("cloudFiles.inferColumnTypes",    "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("multiLine",                      "false")
    .option("mode",                           "PERMISSIVE")
    .load(f"{LANDING}/finance/order_lines/")
    .withColumn("_ingest_time",   current_timestamp())
    .withColumn("_source_file",   input_file_name())
    .withColumn("_source_system", lit("adls_json"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHKPT}/order_lines_raw")
    .option("mergeSchema",        "true")
    .trigger(availableNow=True)
    .toTable("omnioc_dev.bronze.order_lines_raw")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md ## 4 — payments_raw

# COMMAND ----------
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "json")
    .option("cloudFiles.schemaLocation",      f"{CHKPT}/schema/payments_raw")
    .option("cloudFiles.inferColumnTypes",    "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("multiLine",                      "false")
    .option("mode",                           "PERMISSIVE")
    .load(f"{LANDING}/finance/payments/")
    .withColumn("_ingest_time",   current_timestamp())
    .withColumn("_source_file",   input_file_name())
    .withColumn("_source_system", lit("adls_json"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHKPT}/payments_raw")
    .option("mergeSchema",        "true")
    .trigger(availableNow=True)
    .toTable("omnioc_dev.bronze.payments_raw")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md ## 5 — refunds_raw

# COMMAND ----------
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "json")
    .option("cloudFiles.schemaLocation",      f"{CHKPT}/schema/refunds_raw")
    .option("cloudFiles.inferColumnTypes",    "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("multiLine",                      "false")
    .option("mode",                           "PERMISSIVE")
    .load(f"{LANDING}/finance/refunds/")
    .withColumn("_ingest_time",   current_timestamp())
    .withColumn("_source_file",   input_file_name())
    .withColumn("_source_system", lit("adls_json"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHKPT}/refunds_raw")
    .option("mergeSchema",        "true")
    .trigger(availableNow=True)
    .toTable("omnioc_dev.bronze.refunds_raw")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md ## 6 — supplier_contracts_raw (multiLine=true — JSON array file)

# COMMAND ----------
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "json")
    .option("cloudFiles.schemaLocation",      f"{CHKPT}/schema/supplier_contracts_raw")
    .option("cloudFiles.inferColumnTypes",    "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("multiLine",                      "true")   # whole file is one JSON array
    .option("mode",                           "PERMISSIVE")
    .load(f"{LANDING}/supply_chain/supplier_contracts/")
    .withColumn("_ingest_time",   current_timestamp())
    .withColumn("_source_file",   input_file_name())
    .withColumn("_source_system", lit("adls_json"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHKPT}/supplier_contracts_raw")
    .option("mergeSchema",        "true")
    .trigger(availableNow=True)
    .toTable("omnioc_dev.bronze.supplier_contracts_raw")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md ## 7 — cdc_customers_raw

# COMMAND ----------
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "json")
    .option("cloudFiles.schemaLocation",      f"{CHKPT}/schema/cdc_customers_raw")
    .option("cloudFiles.inferColumnTypes",    "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("multiLine",                      "false")
    .option("mode",                           "PERMISSIVE")
    .load(f"{LANDING}/cdc/customers/")
    .withColumn("_ingest_time",   current_timestamp())
    .withColumn("_source_file",   input_file_name())
    .withColumn("_source_system", lit("adls_json"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHKPT}/cdc_customers_raw")
    .option("mergeSchema",        "true")
    .trigger(availableNow=True)
    .toTable("omnioc_dev.bronze.cdc_customers_raw")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md ## 8 — cdc_products_raw

# COMMAND ----------
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "json")
    .option("cloudFiles.schemaLocation",      f"{CHKPT}/schema/cdc_products_raw")
    .option("cloudFiles.inferColumnTypes",    "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("multiLine",                      "false")
    .option("mode",                           "PERMISSIVE")
    .load(f"{LANDING}/cdc/products/")
    .withColumn("_ingest_time",   current_timestamp())
    .withColumn("_source_file",   input_file_name())
    .withColumn("_source_system", lit("adls_json"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHKPT}/cdc_products_raw")
    .option("mergeSchema",        "true")
    .trigger(availableNow=True)
    .toTable("omnioc_dev.bronze.cdc_products_raw")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md ## 9 — poison_pills_raw

# COMMAND ----------
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "json")
    .option("cloudFiles.schemaLocation",      f"{CHKPT}/schema/poison_pills_raw")
    .option("cloudFiles.inferColumnTypes",    "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("multiLine",                      "false")
    .option("mode",                           "PERMISSIVE")    # must be permissive for bad data
    .load(f"{LANDING}/bad_data/")
    .withColumn("_ingest_time",   current_timestamp())
    .withColumn("_source_file",   input_file_name())
    .withColumn("_source_system", lit("adls_json"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHKPT}/poison_pills_raw")
    .option("mergeSchema",        "true")
    .trigger(availableNow=True)
    .toTable("omnioc_dev.bronze.poison_pills_raw")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md ## Summary — Live from Delta Tables

# COMMAND ----------
# DBTITLE 1, Row counts from tables
TABLES = [
    "customers_raw","orders_raw","order_lines_raw","payments_raw",
    "refunds_raw","supplier_contracts_raw",
    "cdc_customers_raw","cdc_products_raw","poison_pills_raw",
]

print("="*65)
print("  03 Bronze JSON — Live Summary")
print("="*65)
print(f"  {'Table':<33} {'Rows':>10}  {'Cols':>5}  Status")
print("-"*65)
for tbl in TABLES:
    df     = spark.table(f"omnioc_dev.bronze.{tbl}")
    rows   = df.count()
    cols   = len(df.columns)
    status = "✅" if rows > 0 else "❌ EMPTY"
    print(f"  {status}  {tbl:<31} {rows:>10,}  {cols:>5}")
print("="*65)

# COMMAND ----------
# DBTITLE 1, Latest ingest time per table
print("  Latest _ingest_time per table:")
print("-"*65)
for tbl in TABLES:
    latest = (spark.table(f"omnioc_dev.bronze.{tbl}")
              .select(spark_max("_ingest_time")).collect()[0][0])
    print(f"    {tbl:<33} {str(latest)}")
print("="*65)

# COMMAND ----------
# DBTITLE 1, Rescued data check — any schema drift caught?
print("  Rescued data (_rescued_data is not null):")
print("-"*65)
for tbl in TABLES:
    df       = spark.table(f"omnioc_dev.bronze.{tbl}")
    if "_rescued_data" in df.columns:
        rescued  = df.filter("_rescued_data is not null").count()
        total    = df.count()
        print(f"    {tbl:<33} {rescued:>6} rescued  /  {total:>10,} total")
print("="*65)

# COMMAND ----------
# DBTITLE 1, CDC operation breakdown
print("  CDC operations breakdown:")
spark.table("omnioc_dev.bronze.cdc_customers_raw").groupBy("operation").count().orderBy("operation").show()
spark.table("omnioc_dev.bronze.cdc_products_raw").groupBy("operation").count().orderBy("operation").show()

# COMMAND ----------
# DBTITLE 1, Poison pill DQ issue breakdown
print("  Poison pill _dq_issue breakdown:")
spark.table("omnioc_dev.bronze.poison_pills_raw").groupBy("_dq_issue").count().orderBy("count", ascending=False).show()
