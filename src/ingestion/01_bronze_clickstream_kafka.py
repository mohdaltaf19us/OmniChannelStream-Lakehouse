# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze Clickstream — Kafka Format (Event Hubs Kafka Endpoint)
# MAGIC Reads from beginning. No Maven library needed. Works on any cluster access mode.

# COMMAND ----------
# DBTITLE 1, Config
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.types import *

CHECKPOINT   = "abfss://checkpoint@omniocdeveusst01.dfs.core.windows.net/bronze/clickstream_kafka"
TARGET_TABLE = "omnioc_dev.bronze.clickstream_raw_kafka"
EH_NAMESPACE = "omnioc-dev-eus-ehns-std-01.servicebus.windows.net"
EH_TOPIC     = "eh-clickstream"

# Namespace-level connection string — strip EntityPath if present
conn_str = dbutils.secrets.get("omnioc-kv-scope", "eventhub-clickstream-connection-string")
if "EntityPath" in conn_str:
    conn_str = ";".join(p for p in conn_str.split(";") if not p.startswith("EntityPath"))

# DBR 15.4 uses standard org.apache.kafka — NOT kafkashaded prefix
JAAS = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="$ConnectionString" password="{conn_str}";'
)

# Payload schema — all types defined here, no inference
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

print(f"✅ Target    : {TARGET_TABLE}")
print(f"✅ Namespace : {EH_NAMESPACE}")
print(f"✅ Topic     : {EH_TOPIC}")
print(f"✅ Reading   : from beginning (earliest)")

# COMMAND ----------
# DBTITLE 1, Read → Cast → Write
(spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",                f"{EH_NAMESPACE}:9093")
    .option("kafka.security.protocol",                "SASL_SSL")
    .option("kafka.sasl.mechanism",                   "PLAIN")
    .option("kafka.sasl.jaas.config",                 JAAS)
    .option("kafka.request.timeout.ms",               "60000")
    .option("kafka.session.timeout.ms",               "30000")
    .option("subscribe",                              EH_TOPIC)
    .option("startingOffsets",                        "earliest")   # read from beginning
    .option("failOnDataLoss",                         "false")
    .option("maxOffsetsPerTrigger",                   "500")
    .load()
    .select(
        from_json(col("value").cast(StringType()), PAYLOAD_SCHEMA).alias("d"),
        col("timestamp").cast(TimestampType()).alias("_eh_enqueued_time"),
        col("partition").cast(StringType()).alias("_eh_partition"),
        col("offset").cast(StringType()).alias("_eh_offset"),
    )
    .select(
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
        "_eh_enqueued_time",
        "_eh_partition",
        "_eh_offset",
        current_timestamp().alias("_ingest_time"),
        lit("eventhubs_kafka").alias("_source_system"),
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
# MAGIC %md ## Summary Stats

# COMMAND ----------
# DBTITLE 1, Row count and ingest times
from pyspark.sql.functions import max as spark_max, min as spark_min

df    = spark.table(TARGET_TABLE)
total = df.count()

print(f"  {'Table':<40}  {TARGET_TABLE}")
print(f"  {'Total rows':<40}  {total:,}")
if total > 0:
    print(f"  {'Latest _ingest_time':<40}  {df.select(spark_max('_ingest_time')).collect()[0][0]}")
    print(f"  {'Earliest EH enqueue':<40}  {df.select(spark_min('_eh_enqueued_time')).collect()[0][0]}")
    print(f"  {'Latest EH enqueue':<40}  {df.select(spark_max('_eh_enqueued_time')).collect()[0][0]}")

# COMMAND ----------
# DBTITLE 1, Event type breakdown
if total > 0:
    df.groupBy("event_type").count().orderBy("count", ascending=False).show()

# COMMAND ----------
# DBTITLE 1, DQ flags — late arrivals and poison pills
if total > 0:
    late  = df.filter(col("_is_late_arrival") == True).count()
    pills = df.filter(col("_is_poison_pill")  == True).count()
    print(f"  Late arrivals  : {late:,}  ({late/total*100:.1f}%)")
    print(f"  Poison pills   : {pills:,}  ({pills/total*100:.1f}%)")

# COMMAND ----------
# DBTITLE 1, Partition distribution
if total > 0:
    df.groupBy("_eh_partition").count().orderBy("_eh_partition").show()

# COMMAND ----------
# DBTITLE 1, Sample rows
if total > 0:
    df.select(
        "event_id","event_type","user_id","geo_country","cart_value","_eh_enqueued_time","_ingest_time"
    ).show(5, truncate=False)
