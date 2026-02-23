# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze Clickstream — Event Hubs Native
# MAGIC Requires Maven library: `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`
# MAGIC Cluster access mode: **Single User**

# COMMAND ----------

# DBTITLE 1, Config
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.types import *

# The secret contains only the SharedAccessKey, not the full connection string
# You need to construct the full connection string with your Event Hub details
EH_SHARED_ACCESS_KEY = dbutils.secrets.get("omnioc-kv-scope", "eventhub-clickstream-connection-string")

# TODO: Replace these placeholders with your actual Event Hub details
EH_NAMESPACE = "omnioc-dev-eus-ehns-std-01"  # e.g., "omnioc-eh-namespace"
EH_KEY_NAME = "eventhub-clickstream-connection-string"              # e.g., "RootManageSharedAccessKey" or "dbx-policy"
EH_ENTITY_PATH = "eh-clickstream"      # e.g., "clickstream-events"

# Construct the full connection string
EH_CONN_STR = f"Endpoint=sb://{EH_NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={EH_KEY_NAME};SharedAccessKey={EH_SHARED_ACCESS_KEY};EntityPath={EH_ENTITY_PATH}"

CHECKPOINT   = "abfss://checkpoint@omniocdeveusst01.dfs.core.windows.net/bronze/clickstream_eh"
TARGET_TABLE = "omnioc_dev.bronze.clickstream_raw_eh"

encrypted = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(EH_CONN_STR)

ehConf = {
    "eventhubs.connectionString"    : encrypted,
    "eventhubs.consumerGroup"       : "cg-dbx-stream",
    "eventhubs.maxEventsPerTrigger" : 500,
    "eventhubs.startingPosition"    : '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}',
}

# Explicit schema — controls ALL types, no inference needed
CLICKSTREAM_SCHEMA = StructType([
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

print(f"✅ Config ready  |  Target: {TARGET_TABLE}")

# COMMAND ----------

# DBTITLE 1, Read → Cast → Write
raw = (spark.readStream
    .format("eventhubs")
    .options(**ehConf)
    .load()
)

parsed = (raw
    # Parse JSON body using explicit schema — types fully controlled here
    .select(
        from_json(col("body").cast(StringType()), CLICKSTREAM_SCHEMA).alias("d"),
        col("enqueuedTime").cast(TimestampType()).alias("_eh_enqueued_time"),
        col("partition").cast(StringType()).alias("_eh_partition"),
        col("offset").cast(StringType()).alias("_eh_offset"),
        col("sequenceNumber").cast(LongType()).alias("_eh_sequence"),
        col("publisher").cast(StringType()).alias("_eh_publisher"),
    )
    .select(
        # All fields come out with correct types from schema above
        col("d.event_id"),
        col("d.event_type"),
        col("d.session_id"),
        col("d.user_id"),
        col("d.device_type"),
        col("d.event_time"),
        col("d.processing_time"),
        col("d.page_url"),
        col("d.referrer_url"),
        col("d.product_id"),
        col("d.category"),
        col("d.search_query"),
        col("d.cart_value"),           # DOUBLE — from schema
        col("d.purchase_amount"),      # DOUBLE — from schema
        col("d.ip_address"),
        col("d.user_agent"),
        col("d.geo_country"),
        col("d.geo_city"),
        col("d.session_page_count"),   # INT — from schema
        col("d._is_late_arrival"),     # BOOLEAN — from schema
        col("d._is_poison_pill"),      # BOOLEAN — from schema
        "_eh_enqueued_time",
        "_eh_partition",
        "_eh_offset",
        "_eh_sequence",
        "_eh_publisher",
        current_timestamp().alias("_ingest_time"),
        lit("eventhubs_native").alias("_source_system"),
    )
)

query = (parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema",        "true")
    .trigger(processingTime="30 seconds")
    .toTable(TARGET_TABLE)
    .awaitTermination()
)

print(f"✅ Streaming to {TARGET_TABLE}  |  trigger: 30s  |  stop with ■")

# COMMAND ----------

# DBTITLE 1, Live monitor (run while streaming)
import time
for i in range(12):
    count = spark.table(TARGET_TABLE).count()
    print(f"  [{i*15:>3}s]  {TARGET_TABLE}  →  {count:,} rows")
    time.sleep(15)
