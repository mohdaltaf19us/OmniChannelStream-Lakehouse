# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Clickstream — Event Hubs Native Format (Requires Maven Library)
# MAGIC Requires: com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22 installed on cluster

# COMMAND ----------
# DBTITLE 1, Config
EH_CONN_STR  = dbutils.secrets.get('omnioc-kv-scope', 'eventhub-clickstream-connection-string')
CHECKPOINT   = "abfss://checkpoint@omniocdeveusst01.dfs.core.windows.net/bronze/clickstream_eh/"
TARGET_TABLE = "omnioc_dev.bronze.clickstream_raw_eh"
EH_NAMESPACE = "omnioc-dev-eus-ehns-std-01"
EH_TOPIC     = "eh-clickstream"

# Encrypt connection string using Event Hubs utils
encrypted = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(EH_CONN_STR)

ehConf = {
    'eventhubs.connectionString'     : encrypted,
    'eventhubs.consumerGroup'        : 'cg-dbx-stream',
    'eventhubs.maxEventsPerTrigger'  : 500,
    'eventhubs.startingPosition'     : '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}',
}

print("✅ Config ready")
print(f"   Namespace : {EH_NAMESPACE}")
print(f"   Topic     : {EH_TOPIC}")
print(f"   Target    : {TARGET_TABLE}")

# COMMAND ----------
# DBTITLE 1, Connect Stream
raw_stream = (spark.readStream
    .format("eventhubs")
    .options(**ehConf)
    .load()
)

print("✅ Stream connected via Event Hubs native format")
raw_stream.printSchema()

# COMMAND ----------
# DBTITLE 1, Define Schema
from pyspark.sql.types import *
from pyspark.sql.functions import *

clickstream_schema = StructType([
    StructField("event_id",            StringType()),
    StructField("event_type",          StringType()),
    StructField("session_id",          StringType()),
    StructField("user_id",             StringType()),
    StructField("device_type",         StringType()),
    StructField("event_time",          StringType()),
    StructField("processing_time",     StringType()),
    StructField("page_url",            StringType()),
    StructField("referrer_url",        StringType()),
    StructField("product_id",          StringType()),
    StructField("category",            StringType()),
    StructField("search_query",        StringType()),
    StructField("cart_value",          DoubleType()),
    StructField("purchase_amount",     DoubleType()),
    StructField("ip_address",          StringType()),
    StructField("user_agent",          StringType()),
    StructField("geo_country",         StringType()),
    StructField("geo_city",            StringType()),
    StructField("session_page_count",  IntegerType()),
    StructField("_is_late_arrival",    BooleanType()),
    StructField("_is_poison_pill",     BooleanType()),
])

print("✅ Schema defined — 21 fields")

# COMMAND ----------
# DBTITLE 1, Parse and Write to Delta
parsed = (raw_stream
    .select(
        from_json(col("body").cast("string"), clickstream_schema).alias("d"),
        col("enqueuedTime").alias("_eh_enqueued_time"),
        col("partition").alias("_eh_partition"),
        col("offset").alias("_eh_offset"),
        col("sequenceNumber").alias("_eh_sequence"),
        col("publisher").alias("_eh_publisher"),
    )
    .select(
        "d.*",
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
    .option("mergeSchema", "true")
    .trigger(processingTime="30 seconds")
    .toTable(TARGET_TABLE)
)

print(f"✅ Stream writing to {TARGET_TABLE}")
print("   Format   : Event Hubs native")
print("   Trigger  : every 30 seconds")
print("   Interrupt kernel (■) to stop")

# COMMAND ----------
# DBTITLE 1, Monitor Row Count
import time
for i in range(10):
    count = spark.table(TARGET_TABLE).count()
    print(f"  [{i*15}s] {TARGET_TABLE} → {count} rows")
    time.sleep(15)
