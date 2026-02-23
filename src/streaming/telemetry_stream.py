# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming: Device Telemetry → Bronze + Silver Alerts
# MAGIC **Source:** Event Hub `device-telemetry` (cg-dbx-stream)
# MAGIC **Pattern:** Streaming + stateful aggregation + threshold alerting via foreachBatch

# COMMAND ----------
import json
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import *

dbutils.widgets.text("catalog",         "omnioc_dev",      "Unity Catalog")
dbutils.widgets.text("storage_account", "omniocdeveusst01","ADLS Account")
dbutils.widgets.text("secret_scope",    "omnioc-kv-scope", "Secret Scope")

CATALOG         = dbutils.widgets.get("catalog")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
SECRET_SCOPE    = dbutils.widgets.get("secret_scope")
EH_NAME         = "device-telemetry"
CONSUMER_GROUP  = "cg-dbx-stream"

def adls(c, p=""): return f"abfss://{c}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{p}"

BRONZE_TABLE  = f"{CATALOG}.bronze.telemetry_raw"
SILVER_TABLE  = f"{CATALOG}.silver.telemetry_alerts"
CHECKPOINT_B  = adls("checkpoint", "streaming/telemetry_bronze/")
CHECKPOINT_S  = adls("checkpoint", "streaming/telemetry_silver/")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")

# COMMAND ----------
TELEMETRY_SCHEMA = StructType([
    StructField("telemetry_id",  StringType(),  True),
    StructField("device_id",     StringType(),  True),
    StructField("device_type",   StringType(),  True),
    StructField("firmware_version", StringType(), True),
    StructField("location_id",   StringType(),  True),
    StructField("event_time",    StringType(),  True),
    StructField("processing_time", StringType(), True),
    StructField("metrics", StructType([
        StructField("cpu_pct",             DoubleType(), True),
        StructField("memory_pct",          DoubleType(), True),
        StructField("disk_pct",            DoubleType(), True),
        StructField("temperature_c",       DoubleType(), True),
        StructField("network_in_kbps",     DoubleType(), True),
        StructField("network_out_kbps",    DoubleType(), True),
        StructField("error_count",         IntegerType(), True),
        StructField("uptime_seconds",      IntegerType(), True),
    ]), True),
    StructField("status",         StringType(), True),
    StructField("alert_triggered", BooleanType(), True),
])

# COMMAND ----------
conn_str = dbutils.secrets.get(scope=SECRET_SCOPE, key="eventhub-clickstream-connection-string")
# Note: For device-telemetry EH, use its own secret if separate namespace; here we reuse for dev
eh_conf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs \
        .EventHubsUtils.encrypt(f"{conn_str};EntityPath={EH_NAME}"),
    "eventhubs.consumerGroup": CONSUMER_GROUP,
    "eventhubs.startingPosition": json.dumps({
        "offset": "@latest", "seqNo": -1, "enqueuedTime": None, "isInclusive": True
    }),
    "eventhubs.maxEventsPerTrigger": 300,
}

# COMMAND ----------
# MAGIC %md ## Parse and flatten metrics struct

raw = (
    spark.readStream.format("eventhubs").options(**eh_conf).load()
    .withColumn("body_str", F.col("body").cast("string"))
    .withColumn("event",    F.from_json("body_str", TELEMETRY_SCHEMA))
    .select("event.*",
            F.col("enqueuedTime").alias("eh_enqueued_time"),
            F.col("sequenceNumber").alias("eh_sequence_no"))
    .withColumn("event_time",       F.to_timestamp("event_time"))
    .withColumn("processing_time",  F.to_timestamp("processing_time"))
    # Flatten metrics struct
    .withColumn("cpu_pct",          F.col("metrics.cpu_pct"))
    .withColumn("memory_pct",       F.col("metrics.memory_pct"))
    .withColumn("disk_pct",         F.col("metrics.disk_pct"))
    .withColumn("temperature_c",    F.col("metrics.temperature_c"))
    .withColumn("network_in_kbps",  F.col("metrics.network_in_kbps"))
    .withColumn("network_out_kbps", F.col("metrics.network_out_kbps"))
    .withColumn("error_count",      F.col("metrics.error_count"))
    .withColumn("uptime_seconds",   F.col("metrics.uptime_seconds"))
    .drop("metrics")
    .withColumn("_ingest_time", F.current_timestamp())
    .withColumn("_source",      F.lit(f"eventhubs/{EH_NAME}"))
)

# Bronze: raw telemetry append
q_bronze = (
    raw.writeStream.format("delta").outputMode("append")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", CHECKPOINT_B)
    .toTable(BRONZE_TABLE)
)

# COMMAND ----------
# MAGIC %md ## Silver alerts — threshold-triggered

ALERT_THRESHOLDS = {
    "cpu_pct":       90.0,   # Alert if CPU > 90%
    "memory_pct":    85.0,   # Alert if memory > 85%
    "disk_pct":      80.0,   # Alert if disk > 80%
    "temperature_c": 75.0,   # Alert if temp > 75°C
}

def create_alerts(batch_df: DataFrame, batch_id: int):
    if batch_df.rdd.isEmpty():
        return
    alerts = batch_df.filter(
        (F.col("cpu_pct")       > ALERT_THRESHOLDS["cpu_pct"])       |
        (F.col("memory_pct")    > ALERT_THRESHOLDS["memory_pct"])    |
        (F.col("disk_pct")      > ALERT_THRESHOLDS["disk_pct"])      |
        (F.col("temperature_c") > ALERT_THRESHOLDS["temperature_c"])
    ).withColumn("alert_reason", F.concat_ws(", ",
        F.when(F.col("cpu_pct")       > ALERT_THRESHOLDS["cpu_pct"],       F.lit("HIGH_CPU")),
        F.when(F.col("memory_pct")    > ALERT_THRESHOLDS["memory_pct"],    F.lit("HIGH_MEM")),
        F.when(F.col("disk_pct")      > ALERT_THRESHOLDS["disk_pct"],      F.lit("HIGH_DISK")),
        F.when(F.col("temperature_c") > ALERT_THRESHOLDS["temperature_c"], F.lit("HIGH_TEMP")),
    )).withColumn("severity", F.when(
        (F.col("cpu_pct") > 98) | (F.col("temperature_c") > 85), "CRITICAL"
    ).when(
        (F.col("cpu_pct") > 90) | (F.col("temperature_c") > 75), "HIGH"
    ).otherwise("MEDIUM"))

    if alerts.count() > 0:
        alerts.write.format("delta").mode("append").saveAsTable(SILVER_TABLE)
        print(f"  Batch {batch_id}: {alerts.count()} alerts generated")

q_alerts = (
    raw.writeStream.foreachBatch(create_alerts)
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", CHECKPOINT_S)
    .start()
)

print(f"✅ Telemetry streams running | Bronze: {BRONZE_TABLE} | Alerts: {SILVER_TABLE}")
