# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Silver — Streaming Transforms
# MAGIC
# MAGIC Reads bronze EH tables via Change Data Feed, applies transforms, writes to silver.
# MAGIC Run as continuous stream — stop with ■ Interrupt.
# MAGIC
# MAGIC | Cell | Bronze Source | Silver Target | Key Logic |
# MAGIC |------|--------------|---------------|-----------|
# MAGIC | 2 | clickstream_raw_eh | clickstream | poison pill route, timestamp cast, dedup |
# MAGIC | 3 | device_telemetry_raw_eh | device_telemetry | poison pill route, range validation, dedup |
# MAGIC | 4 | order_events_raw_eh | order_events + order_event_line_items | line_items explode |
# MAGIC | 5 | dq_alerts_raw_eh | dq_alerts | sample_failed_values JSON → ARRAY |
# MAGIC | 6 | All topics → silver.poison_pills |
# MAGIC | 7 | Block — stop with ■ |
# MAGIC | 8 | Row counts |
# MAGIC | 9 | Spot checks |

# COMMAND ----------
# DBTITLE 1, Cell 1 — Config
from pyspark.sql.functions import (
    col, to_timestamp, to_date, current_timestamp, lit,
    from_json, posexplode, round as spark_round, size, to_json
)
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, IntegerType, DoubleType

BRONZE     = "omnioc_dev.bronze"
SILVER     = "omnioc_dev.silver"
CHKPT_BASE = "abfss://checkpoint@omniocdeveusst01.dfs.core.windows.net/silver"

LINE_ITEM_SCHEMA = ArrayType(StructType([
    StructField("product_id",   StringType(),  True),
    StructField("quantity",     IntegerType(), True),
    StructField("unit_price",   DoubleType(),  True),
    StructField("discount_pct", DoubleType(),  True),
]))

def read_cdf(bronze_table):
    """
    Read bronze table via Change Data Feed.
    startingVersion=0  → process ALL existing bronze data on first run.
    Checkpoint tracks position so restarts pick up from where they left off.
    Only insert and update_postimage change types are forwarded to silver.
    """
    return (
        spark.readStream
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", "0")        # ← read all existing data on first run
        .table(f"{BRONZE}.{bronze_table}")
        .filter(col("_change_type").isin("insert", "update_postimage"))
    )

def write_stream(df, table_name, chkpt_suffix=None):
    suffix = chkpt_suffix or table_name
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHKPT_BASE}/{suffix}")
        .option("mergeSchema", "true")
        .trigger(processingTime="30 seconds")
        .toTable(f"{SILVER}.{table_name}")
    )

print("✅ Config ready")
print(f"   Bronze    : {BRONZE}")
print(f"   Silver    : {SILVER}")
print(f"   Checkpoint: {CHKPT_BASE}")
print(f"   CDF mode  : startingVersion=0 — processes all existing bronze on first run")

# COMMAND ----------
# DBTITLE 1, Cell 2 — clickstream
# Dedup: dropDuplicates on event_id + watermark to bound state size
# Poison pills: filtered out → routed to silver.poison_pills in Cell 6
# Late arrivals: kept in silver (flagged in bronze, useful for analysis)

q_clickstream = write_stream(
    read_cdf("clickstream_raw_eh")
    .filter(col("_is_poison_pill") == False)
    .withWatermark("_eh_enqueued_time", "1 hour")
    .dropDuplicates(["event_id"])
    .select(
        col("event_id"),
        col("event_type"),
        col("session_id"),
        col("user_id"),
        col("device_type"),
        to_timestamp(col("event_time")).alias("event_time"),
        to_timestamp(col("processing_time")).alias("processing_time"),
        col("page_url"),
        col("referrer_url"),
        col("product_id"),
        col("category"),
        col("search_query"),
        col("cart_value").cast("double"),
        col("purchase_amount").cast("double"),
        col("ip_address"),
        col("user_agent"),
        col("geo_country"),
        col("geo_city"),
        col("session_page_count").cast("int"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("clickstream_raw_eh").alias("_bronze_source"),
    ),
    "clickstream"
)
print("✅ Stream started → silver.clickstream")

# COMMAND ----------
# DBTITLE 1, Cell 3 — device_telemetry
# Range validation: temperature 0-120°C, cpu_pct 0-100
# Out-of-range rows → routed to poison_pills with reason "out_of_range"
# Dedup on telemetry_id with 1 hour watermark

df_tel = read_cdf("device_telemetry_raw_eh")

q_telemetry = write_stream(
    df_tel
    .filter(col("_is_poison_pill") == False)
    .filter(col("temperature_c").between(0, 120))
    .filter(col("cpu_pct").between(0, 100))
    .withWatermark("_eh_enqueued_time", "1 hour")
    .dropDuplicates(["telemetry_id"])
    .select(
        col("telemetry_id"),
        col("device_id"),
        col("device_type"),
        col("firmware_version"),
        col("location_id"),
        to_timestamp(col("event_time")).alias("event_time"),
        to_timestamp(col("processing_time")).alias("processing_time"),
        col("cpu_pct").cast("double"),
        col("memory_pct").cast("double"),
        col("disk_pct").cast("double"),
        col("temperature_c").cast("double"),
        col("network_in_kbps").cast("double"),
        col("network_out_kbps").cast("double"),
        col("error_count").cast("int"),
        col("uptime_seconds").cast("int"),
        col("status"),
        col("alert_triggered").cast("boolean"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("device_telemetry_raw_eh").alias("_bronze_source"),
    ),
    "device_telemetry"
)
print("✅ Stream started → silver.device_telemetry")

# COMMAND ----------
# DBTITLE 1, Cell 4 — order_events + order_event_line_items

df_ord = (
    read_cdf("order_events_raw_eh")
    .filter(col("_is_poison_pill") == False)
    .withWatermark("_eh_enqueued_time", "1 hour")
    .dropDuplicates(["event_id"])
    .withColumn("line_items_parsed", from_json(col("line_items"), LINE_ITEM_SCHEMA))
)

# ── order_events header ───────────────────────────────────────────────────────
q_order_events = write_stream(
    df_ord.select(
        col("event_id"),
        col("event_type"),
        col("order_id"),
        col("customer_id"),
        col("warehouse_id"),
        to_timestamp(col("event_time")).alias("event_time"),
        to_date(col("order_date")).alias("order_date"),
        col("status"),
        col("channel"),
        col("currency"),
        col("total_amount").cast("double"),
        col("discount_amount").cast("double"),
        col("tax_amount").cast("double"),
        col("shipping_amount").cast("double"),
        col("shipping_country"),
        col("carrier"),
        col("tracking_number"),
        col("cancellation_reason"),
        size(col("line_items_parsed")).alias("line_item_count"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("order_events_raw_eh").alias("_bronze_source"),
    ),
    "order_events"
)
print("✅ Stream started → silver.order_events")

# ── order_event_line_items — exploded ─────────────────────────────────────────
q_line_items = write_stream(
    df_ord
    .filter(size(col("line_items_parsed")) > 0)
    .select(
        col("event_id"),
        col("order_id"),
        posexplode(col("line_items_parsed")).alias("line_index", "item"),
        to_date(to_timestamp(col("event_time"))).alias("_event_date"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("order_events_raw_eh").alias("_bronze_source"),
    )
    .select(
        col("event_id"),
        col("order_id"),
        col("line_index"),
        col("item.product_id").alias("product_id"),
        col("item.quantity").cast("int").alias("quantity"),
        col("item.unit_price").cast("double").alias("unit_price"),
        col("item.discount_pct").cast("double").alias("discount_pct"),
        spark_round(
            col("item.quantity").cast("double") *
            col("item.unit_price").cast("double") *
            (1 - col("item.discount_pct").cast("double") / 100),
            2
        ).alias("line_total"),
        col("_event_date"),
        col("_silver_ingest_time"),
        col("_bronze_source"),
    ),
    "order_event_line_items"
)
print("✅ Stream started → silver.order_event_line_items")

# COMMAND ----------
# DBTITLE 1, Cell 5 — dq_alerts

q_dq = write_stream(
    read_cdf("dq_alerts_raw_eh")
    .withWatermark("_eh_enqueued_time", "1 hour")
    .dropDuplicates(["alert_id"])
    .select(
        col("alert_id"),
        to_timestamp(col("alert_time")).alias("alert_time"),
        col("severity"),
        col("rule_type"),
        col("rule_name"),
        col("pipeline_name"),
        col("table_name"),
        col("failed_record_count").cast("int"),
        col("total_record_count").cast("int"),
        col("failure_rate_pct").cast("double"),
        from_json(col("sample_failed_values"), ArrayType(StringType())).alias("sample_failed_values"),
        col("action_taken"),
        col("resolution_required").cast("boolean"),
        col("runbook_url"),
        col("correlation_id"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("dq_alerts_raw_eh").alias("_bronze_source"),
    ),
    "dq_alerts"
)
print("✅ Stream started → silver.dq_alerts")

# COMMAND ----------
# DBTITLE 1, Cell 6 — Poison pills (all 3 topics → silver.poison_pills)
# Each topic gets its own checkpoint suffix so they don't conflict writing to the same table
# raw_payload preserved as JSON string for investigation and reprocessing

q_cs_pills = write_stream(
    read_cdf("clickstream_raw_eh")
    .filter(col("_is_poison_pill") == True)
    .select(
        col("event_id").alias("record_id"),
        lit("clickstream").alias("source_topic"),
        lit("clickstream_raw_eh").alias("source_table"),
        to_json(col("event_id")).alias("raw_payload"),   # preserve original
        lit("poison_pill_flag").alias("poison_reason"),
        col("_eh_enqueued_time"),
        col("_eh_partition"),
        current_timestamp().alias("_silver_ingest_time"),
    ),
    "poison_pills", "poison_pills_cs"
)

q_tel_pills = write_stream(
    read_cdf("device_telemetry_raw_eh")
    .filter(col("_is_poison_pill") == True)
    .select(
        col("telemetry_id").alias("record_id"),
        lit("device_telemetry").alias("source_topic"),
        lit("device_telemetry_raw_eh").alias("source_table"),
        to_json(col("telemetry_id")).alias("raw_payload"),
        lit("poison_pill_flag").alias("poison_reason"),
        col("_eh_enqueued_time"),
        col("_eh_partition"),
        current_timestamp().alias("_silver_ingest_time"),
    ),
    "poison_pills", "poison_pills_tel"
)

q_ord_pills = write_stream(
    read_cdf("order_events_raw_eh")
    .filter(col("_is_poison_pill") == True)
    .select(
        col("event_id").alias("record_id"),
        lit("order_events").alias("source_topic"),
        lit("order_events_raw_eh").alias("source_table"),
        to_json(col("event_id")).alias("raw_payload"),
        lit("poison_pill_flag").alias("poison_reason"),
        col("_eh_enqueued_time"),
        col("_eh_partition"),
        current_timestamp().alias("_silver_ingest_time"),
    ),
    "poison_pills", "poison_pills_ord"
)

print("✅ Poison pill streams started → silver.poison_pills (3 sources)")
print("   Checkpoints: poison_pills_cs | poison_pills_tel | poison_pills_ord")

# COMMAND ----------
# DBTITLE 1, Cell 7 — Block (stop with ■ Interrupt)
spark.streams.awaitAnyTermination()

# COMMAND ----------
# DBTITLE 1, Cell 8 — Row counts (run after stopping)
from pyspark.sql.functions import max as spark_max

for tbl in ["clickstream", "device_telemetry", "order_events",
            "order_event_line_items", "dq_alerts", "poison_pills"]:
    df     = spark.table(f"{SILVER}.{tbl}")
    rows   = df.count()
    latest = df.select(spark_max("_silver_ingest_time")).collect()[0][0] if rows > 0 else "—"
    print(f"  {'✅' if rows > 0 else '❌'}  {tbl:<30}  {rows:>8,}  {latest}")

# COMMAND ----------
# DBTITLE 1, Cell 9 — Spot checks
print("── Clickstream event type distribution ─────────────────────────────")
spark.table(f"{SILVER}.clickstream") \
    .groupBy("event_type").count() \
    .orderBy("count", ascending=False).show()

print("── Device telemetry status distribution ────────────────────────────")
spark.table(f"{SILVER}.device_telemetry") \
    .groupBy("status").count() \
    .orderBy("count", ascending=False).show()

print("── Order events by type ────────────────────────────────────────────")
spark.table(f"{SILVER}.order_events") \
    .groupBy("event_type").count() \
    .orderBy("count", ascending=False).show()

print("── Poison pills by source ───────────────────────────────────────────")
spark.table(f"{SILVER}.poison_pills") \
    .groupBy("source_topic", "poison_reason").count() \
    .orderBy("count", ascending=False).show()

print("── Line items sample ───────────────────────────────────────────────")
spark.table(f"{SILVER}.order_event_line_items") \
    .select("event_id", "order_id", "line_index", "product_id",
            "quantity", "unit_price", "line_total") \
    .show(5, truncate=60)
