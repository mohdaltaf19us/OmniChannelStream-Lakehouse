# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze Streaming — Event Hubs Native (All 4 Topics)
# MAGIC
# MAGIC | Hub | Partitions | Target Table |
# MAGIC |-----|-----------|--------------|
# MAGIC | `eh-clickstream` | 2 | `clickstream_raw_eh` |
# MAGIC | `device-telemetry` | 1 | `device_telemetry_raw_eh` |
# MAGIC | `order-events` | 1 | `order_events_raw_eh` |
# MAGIC | `dq-alerts` | 1 | `dq_alerts_raw_eh` |
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Cluster access mode: **Single User**
# MAGIC - Maven library: `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`
# MAGIC - Run Cell 1 (Config) → Cell 2 (Start Streams) → Cell 3 (Monitor)
# MAGIC - Stop all streams with **■ Interrupt**

# COMMAND ----------
# DBTITLE 1, Config — connection strings, schemas, checkpoints
from pyspark.sql.functions import col, from_json, current_timestamp, lit, to_json
from pyspark.sql.types import *

# ── Namespace connection string (dbx-listen-policy — namespace level) ──────────
NS_CONN_STR = dbutils.secrets.get("omnioc-kv-scope", "eventhub-clickstream-connection-string")
EH_NAMESPACE = "omnioc-dev-eus-ehns-std-01"
CG           = "cg-dbx-stream"
CHKPT_BASE   = "abfss://checkpoint@omniocdeveusst01.dfs.core.windows.net/bronze"
CATALOG      = "omnioc_dev.bronze"

def eh_conf(hub_name):
    """Build EH config per hub — appends EntityPath to namespace-level string."""
    conn = NS_CONN_STR.rstrip(";") + f";EntityPath={hub_name}"
    encrypted = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn)
    return {
        "eventhubs.connectionString"    : encrypted,
        "eventhubs.consumerGroup"       : CG,
        "eventhubs.maxEventsPerTrigger" : 500,
        # offset -1 = read from beginning of all partitions
        "eventhubs.startingPosition"    : '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}',
    }

# ── Payload schemas — all types defined here, no inference ─────────────────────

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

# metrics is a nested struct in the producer — flattened to individual columns
# metrics_raw preserves the original JSON for audit
METRICS_SCHEMA = StructType([
    StructField("cpu_pct",          DoubleType(),  True),
    StructField("memory_pct",       DoubleType(),  True),
    StructField("disk_pct",         DoubleType(),  True),
    StructField("temperature_c",    DoubleType(),  True),
    StructField("network_in_kbps",  DoubleType(),  True),
    StructField("network_out_kbps", DoubleType(),  True),
    StructField("error_count",      IntegerType(), True),
    StructField("uptime_seconds",   IntegerType(), True),
])

TELEMETRY_SCHEMA = StructType([
    StructField("telemetry_id",      StringType(),  True),
    StructField("device_id",         StringType(),  True),
    StructField("device_type",       StringType(),  True),
    StructField("firmware_version",  StringType(),  True),
    StructField("location_id",       StringType(),  True),
    StructField("event_time",        StringType(),  True),
    StructField("processing_time",   StringType(),  True),
    StructField("metrics",           METRICS_SCHEMA,True),
    StructField("status",            StringType(),  True),
    StructField("alert_triggered",   BooleanType(), True),
    StructField("_is_late_arrival",  BooleanType(), True),
    StructField("_is_poison_pill",   BooleanType(), True),
])

LINE_ITEM_SCHEMA = StructType([
    StructField("product_id",   StringType(),  True),
    StructField("quantity",     IntegerType(), True),
    StructField("unit_price",   DoubleType(),  True),
    StructField("discount_pct", DoubleType(),  True),
])

ORDER_EVENTS_SCHEMA = StructType([
    StructField("event_id",             StringType(),              True),
    StructField("event_type",           StringType(),              True),
    StructField("order_id",             StringType(),              True),
    StructField("customer_id",          StringType(),              True),
    StructField("warehouse_id",         StringType(),              True),
    StructField("event_time",           StringType(),              True),
    StructField("order_date",           StringType(),              True),
    StructField("status",               StringType(),              True),
    StructField("channel",              StringType(),              True),
    StructField("currency",             StringType(),              True),
    StructField("total_amount",         DoubleType(),              True),
    StructField("discount_amount",      DoubleType(),              True),
    StructField("tax_amount",           DoubleType(),              True),
    StructField("shipping_amount",      DoubleType(),              True),
    StructField("line_items",           ArrayType(LINE_ITEM_SCHEMA),True),
    StructField("shipping_country",     StringType(),              True),
    StructField("carrier",              StringType(),              True),
    StructField("tracking_number",      StringType(),              True),
    StructField("cancellation_reason",  StringType(),              True),
    StructField("_is_poison_pill",      BooleanType(),             True),
])

DQ_ALERTS_SCHEMA = StructType([
    StructField("alert_id",             StringType(),              True),
    StructField("alert_time",           StringType(),              True),
    StructField("severity",             StringType(),              True),
    StructField("rule_type",            StringType(),              True),
    StructField("rule_name",            StringType(),              True),
    StructField("pipeline_name",        StringType(),              True),
    StructField("table_name",           StringType(),              True),
    StructField("failed_record_count",  IntegerType(),             True),
    StructField("total_record_count",   IntegerType(),             True),
    StructField("failure_rate_pct",     DoubleType(),              True),
    StructField("sample_failed_values", ArrayType(StringType()),   True),
    StructField("action_taken",         StringType(),              True),
    StructField("resolution_required",  BooleanType(),             True),
    StructField("runbook_url",          StringType(),              True),
    StructField("correlation_id",       StringType(),              True),
])

print(f"✅ Namespace  : {EH_NAMESPACE}")
print(f"✅ Consumer   : {CG}")
print(f"✅ Checkpoint : {CHKPT_BASE}")
print(f"✅ Schemas    : 4 defined — clickstream, telemetry, order_events, dq_alerts")
print("✅ Config ready — run next cell to start all 4 streams")

# COMMAND ----------
# DBTITLE 1, Start all 4 streams
# ── EH envelope columns — same for every stream ──────────────────────────────
def eh_envelope(raw):
    return (
        raw.select(
            col("body"),
            col("enqueuedTime").cast(TimestampType()).alias("_eh_enqueued_time"),
            col("partition").cast(StringType()).alias("_eh_partition"),
            col("offset").cast(StringType()).alias("_eh_offset"),
            col("sequenceNumber").cast(LongType()).alias("_eh_sequence"),
            col("publisher").cast(StringType()).alias("_eh_publisher"),
        )
    )

# ── 1. Clickstream ─────────────────────────────────────────────────────────────
raw_cs = spark.readStream.format("eventhubs").options(**eh_conf("eh-clickstream")).load()

q_clickstream = (
    eh_envelope(raw_cs)
    .select(
        from_json(col("body").cast(StringType()), CLICKSTREAM_SCHEMA).alias("d"),
        "_eh_enqueued_time", "_eh_partition", "_eh_offset", "_eh_sequence", "_eh_publisher",
    )
    .select(
        "d.event_id", "d.event_type", "d.session_id", "d.user_id", "d.device_type",
        "d.event_time", "d.processing_time", "d.page_url", "d.referrer_url",
        "d.product_id", "d.category", "d.search_query",
        "d.cart_value",            # Double — from schema
        "d.purchase_amount",       # Double — from schema
        "d.ip_address", "d.user_agent", "d.geo_country", "d.geo_city",
        "d.session_page_count",    # Int    — from schema
        "d._is_late_arrival",      # Boolean — from schema
        "d._is_poison_pill",       # Boolean — from schema
        "_eh_enqueued_time", "_eh_partition", "_eh_offset", "_eh_sequence", "_eh_publisher",
        current_timestamp().alias("_ingest_time"),
        lit("eventhubs_native").alias("_source_system"),
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHKPT_BASE}/clickstream_raw_eh")
    .option("mergeSchema", "true")
    .trigger(processingTime="30 seconds")
    .toTable(f"{CATALOG}.clickstream_raw_eh")
)

print("✅ Stream 1 started → clickstream_raw_eh")

# ── 2. Device Telemetry ────────────────────────────────────────────────────────
raw_tel = spark.readStream.format("eventhubs").options(**eh_conf("device-telemetry")).load()

q_telemetry = (
    eh_envelope(raw_tel)
    .select(
        from_json(col("body").cast(StringType()), TELEMETRY_SCHEMA).alias("d"),
        "_eh_enqueued_time", "_eh_partition", "_eh_offset", "_eh_sequence", "_eh_publisher",
    )
    .select(
        "d.telemetry_id", "d.device_id", "d.device_type", "d.firmware_version",
        "d.location_id", "d.event_time", "d.processing_time",
        # Flatten nested metrics struct — types enforced by METRICS_SCHEMA
        col("d.metrics.cpu_pct").alias("cpu_pct"),
        col("d.metrics.memory_pct").alias("memory_pct"),
        col("d.metrics.disk_pct").alias("disk_pct"),
        col("d.metrics.temperature_c").alias("temperature_c"),
        col("d.metrics.network_in_kbps").alias("network_in_kbps"),
        col("d.metrics.network_out_kbps").alias("network_out_kbps"),
        col("d.metrics.error_count").alias("error_count"),
        col("d.metrics.uptime_seconds").alias("uptime_seconds"),
        "d.status", "d.alert_triggered",
        "d._is_late_arrival", "d._is_poison_pill",
        # Preserve original nested metrics as JSON string for audit
        to_json(col("d.metrics")).alias("metrics_raw"),
        "_eh_enqueued_time", "_eh_partition", "_eh_offset", "_eh_sequence", "_eh_publisher",
        current_timestamp().alias("_ingest_time"),
        lit("eventhubs_native").alias("_source_system"),
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHKPT_BASE}/device_telemetry_raw_eh")
    .option("mergeSchema", "true")
    .trigger(processingTime="30 seconds")
    .toTable(f"{CATALOG}.device_telemetry_raw_eh")
)

print("✅ Stream 2 started → device_telemetry_raw_eh")

# ── 3. Order Events ────────────────────────────────────────────────────────────
raw_ord = spark.readStream.format("eventhubs").options(**eh_conf("order-events")).load()

q_orders = (
    eh_envelope(raw_ord)
    .select(
        from_json(col("body").cast(StringType()), ORDER_EVENTS_SCHEMA).alias("d"),
        "_eh_enqueued_time", "_eh_partition", "_eh_offset", "_eh_sequence", "_eh_publisher",
    )
    .select(
        "d.event_id", "d.event_type", "d.order_id", "d.customer_id", "d.warehouse_id",
        "d.event_time", "d.order_date", "d.status", "d.channel", "d.currency",
        "d.total_amount",      # Double — from schema
        "d.discount_amount",   # Double — from schema
        "d.tax_amount",        # Double — from schema
        "d.shipping_amount",   # Double — from schema
        # line_items is ArrayType — serialise to JSON string, parsed in Silver
        to_json(col("d.line_items")).alias("line_items"),
        "d.shipping_country", "d.carrier", "d.tracking_number", "d.cancellation_reason",
        "d._is_poison_pill",   # Boolean — from schema
        "_eh_enqueued_time", "_eh_partition", "_eh_offset", "_eh_sequence", "_eh_publisher",
        current_timestamp().alias("_ingest_time"),
        lit("eventhubs_native").alias("_source_system"),
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHKPT_BASE}/order_events_raw_eh")
    .option("mergeSchema", "true")
    .trigger(processingTime="30 seconds")
    .toTable(f"{CATALOG}.order_events_raw_eh")
)

print("✅ Stream 3 started → order_events_raw_eh")

# ── 4. DQ Alerts ──────────────────────────────────────────────────────────────
raw_dq = spark.readStream.format("eventhubs").options(**eh_conf("dq-alerts")).load()

q_dq = (
    eh_envelope(raw_dq)
    .select(
        from_json(col("body").cast(StringType()), DQ_ALERTS_SCHEMA).alias("d"),
        "_eh_enqueued_time", "_eh_partition", "_eh_offset", "_eh_sequence", "_eh_publisher",
    )
    .select(
        "d.alert_id", "d.alert_time", "d.severity", "d.rule_type", "d.rule_name",
        "d.pipeline_name", "d.table_name",
        "d.failed_record_count",   # Int    — from schema
        "d.total_record_count",    # Int    — from schema
        "d.failure_rate_pct",      # Double — from schema
        # sample_failed_values is ArrayType(StringType) — serialise to JSON string
        to_json(col("d.sample_failed_values")).alias("sample_failed_values"),
        "d.action_taken",
        "d.resolution_required",   # Boolean — from schema
        "d.runbook_url", "d.correlation_id",
        "_eh_enqueued_time", "_eh_partition", "_eh_offset", "_eh_sequence", "_eh_publisher",
        current_timestamp().alias("_ingest_time"),
        lit("eventhubs_native").alias("_source_system"),
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHKPT_BASE}/dq_alerts_raw_eh")
    .option("mergeSchema", "true")
    .trigger(processingTime="30 seconds")
    .toTable(f"{CATALOG}.dq_alerts_raw_eh")
)

print("✅ Stream 4 started → dq_alerts_raw_eh")
print()
print("  All 4 streams running. Next cell will block — stop with ■")

# COMMAND ----------
# DBTITLE 1, Block until interrupted — surfaces real error if any stream crashes
try:
    spark.streams.awaitAnyTermination()
except Exception as e:
    print(f"\n❌ A stream terminated unexpectedly:")
    for q in [q_clickstream, q_telemetry, q_orders, q_dq]:
        if q.exception():
            print(f"  → {q.name}: {q.exception()}")

# COMMAND ----------
# MAGIC %md ## Summary Stats — run after stopping streams

# COMMAND ----------
# DBTITLE 1, Row counts — all 4 tables
from pyspark.sql.functions import max as spark_max, min as spark_min

STREAM_TABLES = [
    "clickstream_raw_eh",
    "device_telemetry_raw_eh",
    "order_events_raw_eh",
    "dq_alerts_raw_eh",
]

print(f"  {'Table':<35}  {'Rows':>8}  Latest _ingest_time")
print("─" * 70)
for tbl in STREAM_TABLES:
    df     = spark.table(f"{CATALOG}.{tbl}")
    rows   = df.count()
    latest = df.select(spark_max("_ingest_time")).collect()[0][0] if rows > 0 else "—"
    status = "✅" if rows > 0 else "❌"
    print(f"  {status}  {tbl:<33}  {rows:>8,}  {latest}")

# COMMAND ----------
# DBTITLE 1, Clickstream — event types and DQ flags
df_cs = spark.table(f"{CATALOG}.clickstream_raw_eh")
if df_cs.count() > 0:
    total = df_cs.count()
    print("  Event type breakdown:")
    df_cs.groupBy("event_type").count().orderBy("count", ascending=False).show()
    late  = df_cs.filter(col("_is_late_arrival") == True).count()
    pills = df_cs.filter(col("_is_poison_pill") == True).count()
    print(f"  Late arrivals : {late:,}  ({late/total*100:.1f}%)")
    print(f"  Poison pills  : {pills:,}  ({pills/total*100:.1f}%)")

# COMMAND ----------
# DBTITLE 1, Telemetry — device status and alerts
df_tel = spark.table(f"{CATALOG}.device_telemetry_raw_eh")
if df_tel.count() > 0:
    print("  Device status breakdown:")
    df_tel.groupBy("status").count().orderBy("count", ascending=False).show()
    print("  Device type breakdown:")
    df_tel.groupBy("device_type").count().orderBy("count", ascending=False).show()
    alerts = df_tel.filter(col("alert_triggered") == True).count()
    print(f"  Alerts triggered : {alerts:,}")

# COMMAND ----------
# DBTITLE 1, Order events — event type and poison pills
df_ord = spark.table(f"{CATALOG}.order_events_raw_eh")
if df_ord.count() > 0:
    print("  Order event type breakdown:")
    df_ord.groupBy("event_type").count().orderBy("count", ascending=False).show()
    pills = df_ord.filter(col("_is_poison_pill") == True).count()
    print(f"  Poison pills : {pills:,}")

# COMMAND ----------
# DBTITLE 1, DQ alerts — severity and action breakdown
df_dq = spark.table(f"{CATALOG}.dq_alerts_raw_eh")
if df_dq.count() > 0:
    print("  Severity breakdown:")
    df_dq.groupBy("severity").count().orderBy("count", ascending=False).show()
    print("  Action taken breakdown:")
    df_dq.groupBy("action_taken").count().orderBy("count", ascending=False).show()
    critical = df_dq.filter(col("resolution_required") == True).count()
    print(f"  Resolution required : {critical:,}")

# COMMAND ----------
# DBTITLE 1, EH partition distribution — all tables
for tbl in STREAM_TABLES:
    df = spark.table(f"{CATALOG}.{tbl}")
    if df.count() > 0:
        print(f"  {tbl} — partition distribution:")
        df.groupBy("_eh_partition").count().orderBy("_eh_partition").show()
