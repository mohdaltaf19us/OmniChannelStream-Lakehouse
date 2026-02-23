# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze Streaming — Kafka Format (EH Kafka Endpoint)
# MAGIC | Cell | Purpose |
# MAGIC |------|---------|
# MAGIC | 1 | Common config — JAAS, schemas, helpers |
# MAGIC | 2 | Stream: clickstream_raw_kafka |
# MAGIC | 3 | Stream: device_telemetry_raw_kafka |
# MAGIC | 4 | Stream: order_events_raw_kafka |
# MAGIC | 5 | Stream: dq_alerts_raw_kafka |
# MAGIC | 6 | Block — keep all streams alive (stop with ■) |
# MAGIC | 7 | Row counts — run after stopping |

# COMMAND ----------

# DBTITLE 1, Cell 1 — Common config, schemas, helpers
from pyspark.sql.functions import col, from_json, current_timestamp, lit, to_json
from pyspark.sql.types import *

# ── Connection ────────────────────────────────────────────────────────────────
_raw = dbutils.secrets.get("omnioc-kv-scope", "eventhub-clickstream-connection-string")
_clean = ";".join(p for p in _raw.rstrip(";").split(";")
                  if p.strip() and not p.strip().startswith("EntityPath"))

EH_BOOTSTRAP = "omnioc-dev-eus-ehns-std-01.servicebus.windows.net:9093"
CG           = "cg-dbx-stream"
CHKPT_BASE   = "abfss://checkpoint@omniocdeveusst01.dfs.core.windows.net/bronze"
CATALOG      = "omnioc_dev.bronze"
SOURCE       = "eventhubs_kafka"

# DBR 15.4 — kafkashaded prefix required for shaded class loader
JAAS = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="$ConnectionString" password="{_clean}";'
)

KAFKA_OPTS = {
    "kafka.bootstrap.servers"  : EH_BOOTSTRAP,
    "kafka.security.protocol"  : "SASL_SSL",
    "kafka.sasl.mechanism"     : "PLAIN",
    "kafka.sasl.jaas.config"   : JAAS,
    "kafka.request.timeout.ms" : "60000",
    "kafka.session.timeout.ms" : "30000",
    "kafka.group.id"           : CG,
    "startingOffsets"          : "earliest",
    "failOnDataLoss"           : "false",
    "maxOffsetsPerTrigger"     : "500",
}

def kafka_stream(topic):
    return (
        spark.readStream.format("kafka")
        .options(**KAFKA_OPTS)
        .option("subscribe", topic)
        .load()
    )

def kafka_envelope(raw):
    return raw.select(
        col("value"),
        col("timestamp").cast(TimestampType()).alias("_eh_enqueued_time"),
        col("partition").cast(StringType()).alias("_eh_partition"),
        col("offset").cast(StringType()).alias("_eh_offset"),
    )

def write_stream(df, table_name):
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHKPT_BASE}/{table_name}")
        .option("mergeSchema", "true")
        .trigger(processingTime="30 seconds")
        .toTable(f"{CATALOG}.{table_name}")
    )

# ── Schemas ───────────────────────────────────────────────────────────────────
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

_METRICS_SCHEMA = StructType([
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
    StructField("telemetry_id",     StringType(),   True),
    StructField("device_id",        StringType(),   True),
    StructField("device_type",      StringType(),   True),
    StructField("firmware_version", StringType(),   True),
    StructField("location_id",      StringType(),   True),
    StructField("event_time",       StringType(),   True),
    StructField("processing_time",  StringType(),   True),
    StructField("metrics",          _METRICS_SCHEMA,True),
    StructField("status",           StringType(),   True),
    StructField("alert_triggered",  BooleanType(),  True),
    StructField("_is_late_arrival", BooleanType(),  True),
    StructField("_is_poison_pill",  BooleanType(),  True),
])

ORDER_EVENTS_SCHEMA = StructType([
    StructField("event_id",            StringType(),                          True),
    StructField("event_type",          StringType(),                          True),
    StructField("order_id",            StringType(),                          True),
    StructField("customer_id",         StringType(),                          True),
    StructField("warehouse_id",        StringType(),                          True),
    StructField("event_time",          StringType(),                          True),
    StructField("order_date",          StringType(),                          True),
    StructField("status",              StringType(),                          True),
    StructField("channel",             StringType(),                          True),
    StructField("currency",            StringType(),                          True),
    StructField("total_amount",        DoubleType(),                          True),
    StructField("discount_amount",     DoubleType(),                          True),
    StructField("tax_amount",          DoubleType(),                          True),
    StructField("shipping_amount",     DoubleType(),                          True),
    StructField("line_items",          ArrayType(StructType([
        StructField("product_id",   StringType(),  True),
        StructField("quantity",     IntegerType(), True),
        StructField("unit_price",   DoubleType(),  True),
        StructField("discount_pct", DoubleType(),  True),
    ])),                                                                       True),
    StructField("shipping_country",    StringType(),                          True),
    StructField("carrier",             StringType(),                          True),
    StructField("tracking_number",     StringType(),                          True),
    StructField("cancellation_reason", StringType(),                          True),
    StructField("_is_poison_pill",     BooleanType(),                         True),
])

DQ_ALERTS_SCHEMA = StructType([
    StructField("alert_id",            StringType(),           True),
    StructField("alert_time",          StringType(),           True),
    StructField("severity",            StringType(),           True),
    StructField("rule_type",           StringType(),           True),
    StructField("rule_name",           StringType(),           True),
    StructField("pipeline_name",       StringType(),           True),
    StructField("table_name",          StringType(),           True),
    StructField("failed_record_count", IntegerType(),          True),
    StructField("total_record_count",  IntegerType(),          True),
    StructField("failure_rate_pct",    DoubleType(),           True),
    StructField("sample_failed_values",ArrayType(StringType()),True),
    StructField("action_taken",        StringType(),           True),
    StructField("resolution_required", BooleanType(),          True),
    StructField("runbook_url",         StringType(),           True),
    StructField("correlation_id",      StringType(),           True),
])

print("✅ Config ready")
print(f"   Bootstrap : {EH_BOOTSTRAP}")
print(f"   Consumer  : {CG}")
print(f"   Checkpoint: {CHKPT_BASE}")
print(f"   Catalog   : {CATALOG}")

# COMMAND ----------

# DBTITLE 1, Cell 2 — Stream: clickstream_raw_kafka
q_clickstream = write_stream(
    kafka_envelope(kafka_stream("eh-clickstream"))
    .select(
        from_json(col("value").cast(StringType()), CLICKSTREAM_SCHEMA).alias("d"),
        "_eh_enqueued_time", "_eh_partition", "_eh_offset",
    )
    .select(
        "d.event_id", "d.event_type", "d.session_id", "d.user_id", "d.device_type",
        "d.event_time", "d.processing_time", "d.page_url", "d.referrer_url",
        "d.product_id", "d.category", "d.search_query",
        "d.cart_value", "d.purchase_amount",
        "d.ip_address", "d.user_agent", "d.geo_country", "d.geo_city",
        "d.session_page_count", "d._is_late_arrival", "d._is_poison_pill",
        "_eh_enqueued_time", "_eh_partition", "_eh_offset",
        current_timestamp().alias("_ingest_time"),
        lit(SOURCE).alias("_source_system"),
    ),
    "clickstream_raw_kafka"
)
print("✅ Stream started → clickstream_raw_kafka")

# COMMAND ----------

# DBTITLE 1, Cell 3 — Stream: device_telemetry_raw_kafka
q_telemetry = write_stream(
    kafka_envelope(kafka_stream("device-telemetry"))
    .select(
        from_json(col("value").cast(StringType()), TELEMETRY_SCHEMA).alias("d"),
        "_eh_enqueued_time", "_eh_partition", "_eh_offset",
    )
    .select(
        "d.telemetry_id", "d.device_id", "d.device_type", "d.firmware_version",
        "d.location_id", "d.event_time", "d.processing_time",
        col("d.metrics.cpu_pct").alias("cpu_pct"),
        col("d.metrics.memory_pct").alias("memory_pct"),
        col("d.metrics.disk_pct").alias("disk_pct"),
        col("d.metrics.temperature_c").alias("temperature_c"),
        col("d.metrics.network_in_kbps").alias("network_in_kbps"),
        col("d.metrics.network_out_kbps").alias("network_out_kbps"),
        col("d.metrics.error_count").alias("error_count"),
        col("d.metrics.uptime_seconds").alias("uptime_seconds"),
        "d.status", "d.alert_triggered", "d._is_late_arrival", "d._is_poison_pill",
        to_json(col("d.metrics")).alias("metrics_raw"),
        "_eh_enqueued_time", "_eh_partition", "_eh_offset",
        current_timestamp().alias("_ingest_time"),
        lit(SOURCE).alias("_source_system"),
    ),
    "device_telemetry_raw_kafka"
)
print("✅ Stream started → device_telemetry_raw_kafka")

# COMMAND ----------

# DBTITLE 1, Cell 4 — Stream: order_events_raw_kafka
q_orders = write_stream(
    kafka_envelope(kafka_stream("order-events"))
    .select(
        from_json(col("value").cast(StringType()), ORDER_EVENTS_SCHEMA).alias("d"),
        "_eh_enqueued_time", "_eh_partition", "_eh_offset",
    )
    .select(
        "d.event_id", "d.event_type", "d.order_id", "d.customer_id", "d.warehouse_id",
        "d.event_time", "d.order_date", "d.status", "d.channel", "d.currency",
        "d.total_amount", "d.discount_amount", "d.tax_amount", "d.shipping_amount",
        to_json(col("d.line_items")).alias("line_items"),   # array → JSON string, parsed in Silver
        "d.shipping_country", "d.carrier", "d.tracking_number", "d.cancellation_reason",
        "d._is_poison_pill",
        "_eh_enqueued_time", "_eh_partition", "_eh_offset",
        current_timestamp().alias("_ingest_time"),
        lit(SOURCE).alias("_source_system"),
    ),
    "order_events_raw_kafka"
)
print("✅ Stream started → order_events_raw_kafka")

# COMMAND ----------

# DBTITLE 1, Cell 5 — Stream: dq_alerts_raw_kafka
q_dq = write_stream(
    kafka_envelope(kafka_stream("dq-alerts"))
    .select(
        from_json(col("value").cast(StringType()), DQ_ALERTS_SCHEMA).alias("d"),
        "_eh_enqueued_time", "_eh_partition", "_eh_offset",
    )
    .select(
        "d.alert_id", "d.alert_time", "d.severity", "d.rule_type", "d.rule_name",
        "d.pipeline_name", "d.table_name",
        "d.failed_record_count", "d.total_record_count", "d.failure_rate_pct",
        to_json(col("d.sample_failed_values")).alias("sample_failed_values"),  # array → JSON string
        "d.action_taken", "d.resolution_required", "d.runbook_url", "d.correlation_id",
        "_eh_enqueued_time", "_eh_partition", "_eh_offset",
        current_timestamp().alias("_ingest_time"),
        lit(SOURCE).alias("_source_system"),
    ),
    "dq_alerts_raw_kafka"
)
print("✅ Stream started → dq_alerts_raw_kafka")

# COMMAND ----------

# DBTITLE 1, Cell 6 — Block (stop with ■ Interrupt)
spark.streams.awaitAnyTermination()

# COMMAND ----------

# DBTITLE 1, Cell 7 — Row counts (run after stopping)
from pyspark.sql.functions import max as spark_max

for tbl in ["clickstream_raw_kafka", "device_telemetry_raw_kafka",
            "order_events_raw_kafka", "dq_alerts_raw_kafka"]:
    df     = spark.table(f"{CATALOG}.{tbl}")
    rows   = df.count()
    latest = df.select(spark_max("_ingest_time")).collect()[0][0] if rows > 0 else "—"
    print(f"  {'✅' if rows > 0 else '❌'}  {tbl:<38}  {rows:>8,}  {latest}")
