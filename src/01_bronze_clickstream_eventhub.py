# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: Clickstream Event Hubs → Delta
# MAGIC **Source:** Azure Event Hubs `eh-clickstream` (consumer group `cg-dbx-stream`)
# MAGIC **Sink:**   `omnioc_dev.bronze.clickstream_raw` (Delta Lake, append mode)
# MAGIC **Pattern:** Structured Streaming · Micro-batch every 30 seconds · Checkpoint ensures exactly-once

# COMMAND ----------
import json
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, IntegerType, TimestampType,
)

# ── Widget parameters (set by DABs job, or manually in interactive) ──────────
dbutils.widgets.text("catalog",          "omnioc_dev",            "Unity Catalog")
dbutils.widgets.text("storage_account",  "omniocdeveusst01",      "ADLS Account")
dbutils.widgets.text("secret_scope",     "omnioc-kv-scope",       "Databricks Secret Scope")
dbutils.widgets.text("trigger_interval", "30 seconds",            "Streaming trigger interval")
dbutils.widgets.text("eventhub_name",    "eh-clickstream",        "Event Hub name")
dbutils.widgets.text("consumer_group",   "cg-dbx-stream",         "Consumer group")

CATALOG          = dbutils.widgets.get("catalog")
STORAGE_ACCOUNT  = dbutils.widgets.get("storage_account")
SECRET_SCOPE     = dbutils.widgets.get("secret_scope")
TRIGGER_INTERVAL = dbutils.widgets.get("trigger_interval")
EH_NAME          = dbutils.widgets.get("eventhub_name")
CONSUMER_GROUP   = dbutils.widgets.get("consumer_group")

BRONZE_PATH      = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/clickstream/"
CHECKPOINT_PATH  = f"abfss://checkpoint@{STORAGE_ACCOUNT}.dfs.core.windows.net/clickstream_bronze/"
TABLE            = f"{CATALOG}.bronze.clickstream_raw"

print(f"Catalog       : {CATALOG}")
print(f"Event Hub     : {EH_NAME} / {CONSUMER_GROUP}")
print(f"Bronze path   : {BRONZE_PATH}")
print(f"Checkpoint    : {CHECKPOINT_PATH}")
print(f"Target table  : {TABLE}")

# COMMAND ----------
# MAGIC %md ## 1 — Create Bronze Schema

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
print(f"✅ {CATALOG}.bronze schema ready")

# COMMAND ----------
# MAGIC %md ## 2 — Event Hubs Connection

connection_string = dbutils.secrets.get(
    scope=SECRET_SCOPE,
    key="eventhub-clickstream-connection-string"
)
eh_connection = f"{connection_string};EntityPath={EH_NAME}"

eh_conf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs \
        .EventHubsUtils.encrypt(eh_connection),
    "eventhubs.consumerGroup": CONSUMER_GROUP,
    "eventhubs.startingPosition": json.dumps({
        "offset": "@latest",
        "seqNo": -1,
        "enqueuedTime": None,
        "isInclusive": True,
    }),
    "eventhubs.maxEventsPerTrigger": 500,
}
print("✅ Event Hubs config ready")

# COMMAND ----------
# MAGIC %md ## 3 — Clickstream Schema

clickstream_schema = StructType([
    StructField("event_id",           StringType(),  True),
    StructField("event_type",         StringType(),  True),
    StructField("session_id",         StringType(),  True),
    StructField("user_id",            StringType(),  True),
    StructField("device_type",        StringType(),  True),
    StructField("event_time",         StringType(),  True),
    StructField("processing_time",    StringType(),  True),
    StructField("page_url",           StringType(),  True),
    StructField("referrer_url",       StringType(),  True),
    StructField("product_id",         StringType(),  True),
    StructField("category",           StringType(),  True),
    StructField("search_query",       StringType(),  True),
    StructField("cart_value",         DoubleType(),  True),
    StructField("purchase_amount",    DoubleType(),  True),
    StructField("ip_address",         StringType(),  True),
    StructField("user_agent",         StringType(),  True),
    StructField("geo_country",        StringType(),  True),
    StructField("geo_city",           StringType(),  True),
    StructField("session_page_count", IntegerType(), True),
    StructField("_is_late_arrival",   BooleanType(), True),
    StructField("_is_poison_pill",    BooleanType(), True),
])

# COMMAND ----------
# MAGIC %md ## 4 — Read → Parse → Write Stream

raw_stream = (
    spark.readStream
    .format("eventhubs")
    .options(**eh_conf)
    .load()
)

parsed_stream = (
    raw_stream
    .withColumn("body_str", F.col("body").cast("string"))
    .withColumn("event",    F.from_json("body_str", clickstream_schema))
    .select(
        "event.*",
        F.col("enqueuedTime").alias("eh_enqueued_time"),
        F.col("offset").cast("string").alias("eh_offset"),
        F.col("sequenceNumber").alias("eh_sequence_no"),
        F.col("partitionId").alias("eh_partition_id"),
    )
    .withColumn("event_time",      F.to_timestamp("event_time"))
    .withColumn("processing_time", F.to_timestamp("processing_time"))
    .withColumn("_ingest_time",    F.current_timestamp())
    .withColumn("_source",         F.lit(f"eventhubs/{EH_NAME}"))
    .withColumn("_parse_failed",   F.col("event_id").isNull())
    # Partition by date for efficient queries
    .withColumn("_ingest_date",    F.to_date("_ingest_time"))
)

query = (
    parsed_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .partitionBy("_ingest_date")
    .toTable(TABLE)
)

print(f"✅ Stream started | Query ID: {query.id}")
print(f"   Trigger : {TRIGGER_INTERVAL}")
print(f"   Table   : {TABLE}")
print(f"   Run query.stop() to stop the stream")

# COMMAND ----------
# MAGIC %md ## 5 — Monitor (run while stream active)

# Uncomment to check stream status interactively
# import time
# for _ in range(10):
#     if not query.isActive:
#         print("Stream stopped")
#         break
#     prog = query.recentProgress
#     if prog:
#         p = prog[-1]
#         print(f"Batch {p.get('batchId'):>4} | Rows: {p.get('numInputRows',0):>6,} | "
#               f"Processing rate: {p.get('processedRowsPerSecond',0):.1f} rows/s")
#     time.sleep(10)
