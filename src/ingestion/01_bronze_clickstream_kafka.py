# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze Clickstream — Kafka Format (No Library Needed)
# MAGIC - No Maven library required — uses built-in Kafka connector
# MAGIC - Checkpoint : abfss://checkpoint@omniocdeveusst01.dfs.core.windows.net/bronze/clickstream_kafka/
# MAGIC - Target     : omnioc_dev.bronze.clickstream_raw_kafka

# COMMAND ----------

# DBTITLE 1, Config
EH_CONN_STR  = dbutils.secrets.get('omnioc-kv-scope', 'eventhub-clickstream-connection-string')
CHECKPOINT   = "abfss://checkpoint@omniocdeveusst01.dfs.core.windows.net/bronze/clickstream_kafka/"
TARGET_TABLE = "omnioc_dev.bronze.clickstream_raw_kafka"

JAAS = (
    'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="$ConnectionString" '
    f'password="{EH_CONN_STR}";'
)

print("✅ Config ready")
print(f"   Format     : Kafka (built-in)")
print(f"   Consumer   : cg-dbx-stream")
print(f"   Checkpoint : {CHECKPOINT}")
print(f"   Target     : {TARGET_TABLE}")

# COMMAND ----------

# DBTITLE 1, Connect Stream
raw_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",
            "omnioc-dev-eus-ehns-std-01.servicebus.windows.net:9093")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism",    "PLAIN")
    .option("kafka.sasl.jaas.config",  JAAS)
    .option("subscribe",               "eh-clickstream")
    .option("kafka.group.id",          "cg-dbx-stream")
    .option("startingOffsets",         "earliest")
    .option("failOnDataLoss",          "false")
    .option("maxOffsetsPerTrigger",    "500")
    .load()
)

print("✅ Stream connected via Kafka format")
raw_stream.printSchema()

# COMMAND ----------

# DBTITLE 1, Define Schema
from pyspark.sql.types import *
from pyspark.sql.functions import *

clickstream_schema = StructType([
    StructField("event_id",           StringType()),
    StructField("event_type",         StringType()),
    StructField("session_id",         StringType()),
    StructField("user_id",            StringType()),
    StructField("device_type",        StringType()),
    StructField("event_time",         StringType()),
    StructField("processing_time",    StringType()),
    StructField("page_url",           StringType()),
    StructField("referrer_url",       StringType()),
    StructField("product_id",         StringType()),
    StructField("category",           StringType()),
    StructField("search_query",       StringType()),
    StructField("cart_value",         DoubleType()),
    StructField("purchase_amount",    DoubleType()),
    StructField("ip_address",         StringType()),
    StructField("user_agent",         StringType()),
    StructField("geo_country",        StringType()),
    StructField("geo_city",           StringType()),
    StructField("session_page_count", IntegerType()),
    StructField("_is_late_arrival",   BooleanType()),
    StructField("_is_poison_pill",    BooleanType()),
])

print("✅ Schema defined — 21 fields")

# COMMAND ----------

# DBTITLE 1, Parse and Write to Delta
parsed = (raw_stream
    .select(
        from_json(col("value").cast("string"), clickstream_schema).alias("d"),
        col("timestamp").alias("_eh_enqueued_time"),          # Kafka column name
        col("partition").cast("string").alias("_eh_partition"),
        col("offset").cast("string").alias("_eh_offset"),
    )
    .select(
        "d.*",
        "_eh_enqueued_time",
        "_eh_partition",
        "_eh_offset",
        current_timestamp().alias("_ingest_time"),
        lit("eventhubs_kafka").alias("_source_system"),       # identifies this ingestion path
    )
)

query = (parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)                 # Kafka-specific checkpoint
    .option("mergeSchema", "true")
    .trigger(processingTime="30 seconds")
    .toTable(TARGET_TABLE)
)

print(f"✅ Stream writing to {TARGET_TABLE}")
print(f"   Payload column  : value")
print(f"   Timestamp column: timestamp")
print(f"   Checkpoint      : {CHECKPOINT}")
print("   Interrupt kernel (■) to stop")

# COMMAND ----------

# DBTITLE 1, Monitor
import time
for i in range(2):
    count = spark.table(TARGET_TABLE).count()
    print(f"  [{i*15}s] {TARGET_TABLE} → {count:,} rows")
    time.sleep(15)
