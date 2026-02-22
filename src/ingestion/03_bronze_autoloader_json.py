# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Bronze Autoloader — JSON Files
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
# MAGIC %md ## 1 — customers_raw

# COMMAND ----------
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "json")
    .option("cloudFiles.schemaLocation",      f"{CHKPT}/schema/customers_raw")
    .option("cloudFiles.inferColumnTypes",    "false")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("multiLine",                      "false")
    .option("mode",                           "PERMISSIVE")
    .load(f"{LANDING}/finance/customers/")
    # ── Type casts ──────────────────────────────────────────────────────────
    .withColumn("loyalty_points",      col("loyalty_points").cast(IntegerType()))
    .withColumn("is_email_verified",   col("is_email_verified").cast(BooleanType()))
    .withColumn("is_active",           col("is_active").cast(BooleanType()))
    # ── Metadata ────────────────────────────────────────────────────────────
    .withColumn("_ingest_time",        current_timestamp())
    .withColumn("_source_file",        input_file_name())
    .withColumn("_source_system",      lit("adls_json"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",      f"{CHKPT}/customers_raw")
    .option("mergeSchema",             "true")
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
    .option("cloudFiles.inferColumnTypes",    "false")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("multiLine",                      "false")
    .option("mode",                           "PERMISSIVE")
    .load(f"{LANDING}/finance/orders/")
    # ── Type casts ──────────────────────────────────────────────────────────
    .withColumn("total_amount",        col("total_amount").cast(DoubleType()))
    .withColumn("discount_amount",     col("discount_amount").cast(DoubleType()))
    .withColumn("tax_amount",          col("tax_amount").cast(DoubleType()))
    .withColumn("shipping_amount",     col("shipping_amount").cast(DoubleType()))
    # ── Metadata ────────────────────────────────────────────────────────────
    .withColumn("_ingest_time",        current_timestamp())
    .withColumn("_source_file",        input_file_name())
    .withColumn("_source_system",      lit("adls_json"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",      f"{CHKPT}/orders_raw")
    .option("mergeSchema",             "true")
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
    .option("cloudFiles.inferColumnTypes",    "false")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("multiLine",                      "false")
    .option("mode",                           "PERMISSIVE")
    .load(f"{LANDING}/finance/order_lines/")
    # ── Type casts ──────────────────────────────────────────────────────────
    .withColumn("quantity",            col("quantity").cast(IntegerType()))
    .withColumn("unit_price",          col("unit_price").cast(DoubleType()))
    .withColumn("discount_pct",        col("discount_pct").cast(DoubleType()))
    .withColumn("line_total",          col("line_total").cast(DoubleType()))
    .withColumn("is_gift",             col("is_gift").cast(BooleanType()))
    # ── Metadata ────────────────────────────────────────────────────────────
    .withColumn("_ingest_time",        current_timestamp())
    .withColumn("_source_file",        input_file_name())
    .withColumn("_source_system",      lit("adls_json"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",      f"{CHKPT}/order_lines_raw")
    .option("mergeSchema",             "true")
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
    .option("cloudFiles.inferColumnTypes",    "false")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("multiLine",                      "false")
    .option("mode",                           "PERMISSIVE")
    .load(f"{LANDING}/finance/payments/")
    # ── Type casts ──────────────────────────────────────────────────────────
    .withColumn("amount",              col("amount").cast(DoubleType()))
    # ── Metadata ────────────────────────────────────────────────────────────
    .withColumn("_ingest_time",        current_timestamp())
    .withColumn("_source_file",        input_file_name())
    .withColumn("_source_system",      lit("adls_json"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",      f"{CHKPT}/payments_raw")
    .option("mergeSchema",             "true")
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
    .option("cloudFiles.inferColumnTypes",    "false")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("multiLine",                      "false")
    .option("mode",                           "PERMISSIVE")
    .load(f"{LANDING}/finance/refunds/")
    # ── Type casts ──────────────────────────────────────────────────────────
    .withColumn("refund_amount",       col("refund_amount").cast(DoubleType()))
    # ── Metadata ────────────────────────────────────────────────────────────
    .withColumn("_ingest_time",        current_timestamp())
    .withColumn("_source_file",        input_file_name())
    .withColumn("_source_system",      lit("adls_json"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",      f"{CHKPT}/refunds_raw")
    .option("mergeSchema",             "true")
    .trigger(availableNow=True)
    .toTable("omnioc_dev.bronze.refunds_raw")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md ## 6 — supplier_contracts_raw (multiLine=true)

# COMMAND ----------
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "json")
    .option("cloudFiles.schemaLocation",      f"{CHKPT}/schema/supplier_contracts_raw")
    .option("cloudFiles.inferColumnTypes",    "false")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("multiLine",                      "true")          # whole file is one JSON array
    .option("mode",                           "PERMISSIVE")
    .load(f"{LANDING}/supply_chain/supplier_contracts/")
    # ── Type casts ──────────────────────────────────────────────────────────
    .withColumn("total_value_usd",     col("total_value_usd").cast(DoubleType()))
    .withColumn("auto_renewal",        col("auto_renewal").cast(BooleanType()))
    .withColumn("is_active",           col("is_active").cast(BooleanType()))
    # ── Metadata ────────────────────────────────────────────────────────────
    .withColumn("_ingest_time",        current_timestamp())
    .withColumn("_source_file",        input_file_name())
    .withColumn("_source_system",      lit("adls_json"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",      f"{CHKPT}/supplier_contracts_raw")
    .option("mergeSchema",             "true")
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
    .option("cloudFiles.inferColumnTypes",    "false")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("multiLine",                      "false")
    .option("mode",                           "PERMISSIVE")
    .load(f"{LANDING}/cdc/customers/")
    # ── No numeric/bool casts needed — before/after are JSON strings, rest are strings
    # ── Metadata ────────────────────────────────────────────────────────────
    .withColumn("_ingest_time",        current_timestamp())
    .withColumn("_source_file",        input_file_name())
    .withColumn("_source_system",      lit("adls_json"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",      f"{CHKPT}/cdc_customers_raw")
    .option("mergeSchema",             "true")
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
    .option("cloudFiles.inferColumnTypes",    "false")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("multiLine",                      "false")
    .option("mode",                           "PERMISSIVE")
    .load(f"{LANDING}/cdc/products/")
    # ── No numeric/bool casts needed — before/after are JSON strings, rest are strings
    # ── Metadata ────────────────────────────────────────────────────────────
    .withColumn("_ingest_time",        current_timestamp())
    .withColumn("_source_file",        input_file_name())
    .withColumn("_source_system",      lit("adls_json"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",      f"{CHKPT}/cdc_products_raw")
    .option("mergeSchema",             "true")
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
    .option("cloudFiles.inferColumnTypes",    "false")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn",   "_rescued_data")
    .option("multiLine",                      "false")
    .option("mode",                           "PERMISSIVE")    # essential — records are intentionally bad
    .load(f"{LANDING}/bad_data/")
    # ── total_amount kept as STRING intentionally — contains "NOT_A_NUMBER" test values
    # ── No other numeric casts needed for poison pills — DDL already uses STRING for all fields
    # ── Metadata ────────────────────────────────────────────────────────────
    .withColumn("_ingest_time",        current_timestamp())
    .withColumn("_source_file",        input_file_name())
    .withColumn("_source_system",      lit("adls_json"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",      f"{CHKPT}/poison_pills_raw")
    .option("mergeSchema",             "true")
    .trigger(availableNow=True)
    .toTable("omnioc_dev.bronze.poison_pills_raw")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md ## Summary — Live from Delta Tables

# COMMAND ----------
# DBTITLE 1, Row counts + column counts
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
for tbl in TABLES:
    latest = (spark.table(f"omnioc_dev.bronze.{tbl}")
              .select(spark_max("_ingest_time")).collect()[0][0])
    print(f"  {tbl:<33}  last ingest: {latest}")

# COMMAND ----------
# DBTITLE 1, Rescued data — any schema drift caught?
print("  Rescued rows per table:")
print("-"*50)
for tbl in TABLES:
    df      = spark.table(f"omnioc_dev.bronze.{tbl}")
    rescued = df.filter("_rescued_data is not null").count()
    total   = df.count()
    print(f"  {tbl:<33}  rescued: {rescued:>5}  /  total: {total:>10,}")

# COMMAND ----------
# DBTITLE 1, CDC operations breakdown
print("  cdc_customers_raw operations:")
spark.table("omnioc_dev.bronze.cdc_customers_raw").groupBy("operation").count().orderBy("operation").show()

print("  cdc_products_raw operations:")
spark.table("omnioc_dev.bronze.cdc_products_raw").groupBy("operation").count().orderBy("operation").show()

# COMMAND ----------
# DBTITLE 1, Poison pill DQ breakdown
print("  poison_pills_raw _dq_issue breakdown:")
spark.table("omnioc_dev.bronze.poison_pills_raw").groupBy("_dq_issue").count().orderBy("count", ascending=False).show()
