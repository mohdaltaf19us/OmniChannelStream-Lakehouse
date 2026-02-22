# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze Clickstream — Event Hubs Native
# MAGIC Reads from beginning. Requires Maven: `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`
# MAGIC Cluster access mode: **Single User**

# COMMAND ----------
# DBTITLE 1, Config
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.types import *

CHECKPOINT   = "abfss://checkpoint@omniocdeveusst01.dfs.core.windows.net/bronze/clickstream_eh"
TARGET_TABLE = "omnioc_dev.bronze.clickstream_raw_eh"

# Get connection string — append EntityPath if missing (namespace-level string won't parse)
conn_str = dbutils.secrets.get("omnioc-kv-scope", "eventhub-clickstream-connection-string")
if "EntityPath" not in conn_str:
    conn_str = conn_str.rstrip(";") + ";EntityPath=eh-clickstream"

encrypted = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn_str)

ehConf = {
    "eventhubs.connectionString"    : encrypted,
    "eventhubs.consumerGroup"       : "cg-dbx-stream",
    "eventhubs.maxEventsPerTrigger" : 500,
    "eventhubs.startingPosition"    : '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}',  # read from beginning
}

# Payload schema — controls all types, no inference
PAYLOAD_SCHEMA = StructType([
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

print(f"✅ Target : {TARGET_TABLE}")
print(f"✅ Reading: from beginning (offset -1)")

# COMMAND ----------
# DBTITLE 1, Read → Cast → Write
(spark.readStream
    .format("eventhubs")
    .options(**ehConf)
    .load()
    .select(
        from_json(col("body").cast(StringType()), PAYLOAD_SCHEMA).alias("d"),
        col("enqueuedTime").cast(TimestampType()).alias("_eh_enqueued_time"),
        col("partition").cast(StringType()).alias("_eh_partition"),
        col("offset").cast(StringType()).alias("_eh_offset"),
        col("sequenceNumber").cast(LongType()).alias("_eh_sequence"),
    )
    .select(
        # Payload fields — types enforced by PAYLOAD_SCHEMA above
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
        col("d.cart_value"),            # Double — from schema
        col("d.purchase_amount"),       # Double — from schema
        col("d.ip_address"),
        col("d.user_agent"),
        col("d.geo_country"),
        col("d.geo_city"),
        col("d.session_page_count"),    # Int — from schema
        col("d._is_late_arrival"),      # Boolean — from schema
        col("d._is_poison_pill"),       # Boolean — from schema
        # EH metadata
        "_eh_enqueued_time",
        "_eh_partition",
        "_eh_offset",
        "_eh_sequence",
        # Audit
        current_timestamp().alias("_ingest_time"),
        lit("eventhubs_native").alias("_source_system"),
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema",        "true")
    .trigger(processingTime="30 seconds")
    .toTable(TARGET_TABLE)
)

print(f"✅ Streaming started → {TARGET_TABLE}  |  stop with ■")

# COMMAND ----------
# MAGIC %md ## Summary Stats (run while streaming or after stopping)

# COMMAND ----------
# DBTITLE 1, Row count and latest ingest
from pyspark.sql.functions import max as spark_max, min as spark_min, count, when

df    = spark.table(TARGET_TABLE)
total = df.count()

print(f"  {'Table':<40}  {TARGET_TABLE}")
print(f"  {'Total rows':<40}  {total:,}")

if total > 0:
    latest = df.select(spark_max("_ingest_time")).collect()[0][0]
    oldest = df.select(spark_min("_eh_enqueued_time")).collect()[0][0]
    newest = df.select(spark_max("_eh_enqueued_time")).collect()[0][0]
    print(f"  {'Latest _ingest_time':<40}  {latest}")
    print(f"  {'Earliest EH enqueue time':<40}  {oldest}")
    print(f"  {'Latest EH enqueue time':<40}  {newest}")

# COMMAND ----------
# DBTITLE 1, Event type breakdown
if total > 0:
    print("  Event types:")
    df.groupBy("event_type").count().orderBy("count", ascending=False).show()

# COMMAND ----------
# DBTITLE 1, DQ flags
if total > 0:
    late  = df.filter(col("_is_late_arrival") == True).count()
    pills = df.filter(col("_is_poison_pill")  == True).count()
    print(f"  Late arrivals  : {late:,}  ({late/total*100:.1f}%)")
    print(f"  Poison pills   : {pills:,}  ({pills/total*100:.1f}%)")

# COMMAND ----------
# DBTITLE 1, Partition distribution
if total > 0:
    print("  Partition distribution:")
    df.groupBy("_eh_partition").count().orderBy("_eh_partition").show()

# COMMAND ----------
# DBTITLE 1, Sample rows
if total > 0:
    df.select(
        "event_id","event_type","user_id","geo_country","cart_value","_eh_enqueued_time","_ingest_time"
    ).show(5, truncate=False)
