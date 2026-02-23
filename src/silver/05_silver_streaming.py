# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Silver — Streaming Transforms
# MAGIC
# MAGIC **Transformation techniques demonstrated (streaming-specific §14 + others):**
# MAGIC
# MAGIC | Technique | Section | Applied To |
# MAGIC |-----------|---------|-----------|
# MAGIC | `window()` event-time tumbling | §14 Streaming | clickstream — session activity buckets |
# MAGIC | `stream-static join` + `broadcast` hint | §14 + §3 | clickstream — enrich with product lookup |
# MAGIC | `foreachBatch` | §14 Streaming | order_events — custom upsert with dedup logic |
# MAGIC | `dropDuplicates` + `withWatermark` | §14 Streaming | all streams — exactly-once semantics |
# MAGIC | `transform()` higher-order | §8 Array | line_items — uppercase product_id in array |
# MAGIC | `filter()` higher-order array | §8 Array | dq_alerts — remove null entries from array |
# MAGIC | `array_distinct` | §8 Array | dq_alerts — deduplicate sample_failed_values |
# MAGIC | `posexplode_outer` | §7 Explode | order_events — explode line items with index |
# MAGIC | `from_json / schema_of_json` | §10 Schema | all streams — parse JSON payloads |
# MAGIC | `regexp_extract` | §9 String | clickstream — extract UTM source from URL |
# MAGIC | `date_trunc` | §9 Date | order_events — hour bucket for event time |
# MAGIC | `coalesce` | §9 Null | device_telemetry — null-safe metric fallback |
# MAGIC | `when/otherwise` | §1 Shaping | device_telemetry — composite health score |

# COMMAND ----------
# DBTITLE 1, Cell 1 — Config & schemas
from pyspark.sql.functions import (
    col, to_timestamp, to_date, current_timestamp, lit,
    from_json, to_json, posexplode_outer,
    round as spark_round, size, when,
    # String
    regexp_extract, lower, upper, trim,
    # Array / higher-order
    transform, filter as array_filter, array_distinct, array_compact,
    # Date
    window, date_trunc, hour,
    # Null
    coalesce,
    # Aggregation
    count as spark_count, sum as spark_sum, avg as spark_avg,
)
from pyspark.sql.types import (
    ArrayType, StringType, StructType, StructField,
    IntegerType, DoubleType, TimestampType
)

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
    startingVersion=0  → process all existing data on first run.
    Checkpoint tracks position so restarts pick up from where they left off.
    """
    return (
        spark.readStream
        .format("delta")
        .option("readChangeFeed",  "true")
        .option("startingVersion", "0")
        .table(f"{BRONZE}.{bronze_table}")
        .filter(col("_change_type").isin("insert", "update_postimage"))
    )

def write_stream(df, table_name, chkpt_suffix=None, mode="append"):
    suffix = chkpt_suffix or table_name
    return (
        df.writeStream
        .format("delta")
        .outputMode(mode)
        .option("checkpointLocation", f"{CHKPT_BASE}/{suffix}")
        .option("mergeSchema", "true")
        .trigger(processingTime="30 seconds")
        .toTable(f"{SILVER}.{table_name}")
    )

print("✅ Config ready")
print(f"   Bronze    : {BRONZE}")
print(f"   Silver    : {SILVER}")
print(f"   Checkpoint: {CHKPT_BASE}")

# COMMAND ----------
# DBTITLE 1, Cell 2 — clickstream
# Techniques:
#   withWatermark + dropDuplicates — exactly-once event_id dedup
#   regexp_extract                 — pull UTM source from page_url query string
#   when/otherwise                 — coarser device_category grouping
#   hour()                         — extract event hour for intraday analysis
#   stream-static join + broadcast — enrich events with product name lookup

# ── Static products table for enrichment (broadcast hint for efficiency) ───────
df_products_static = spark.read.table(f"{SILVER}.products") \
    .select("product_id", "product_name", "category_l1", "price_tier") \
    .hint("broadcast")                         # broadcast small dim to all executors

df_clickstream_base = (
    read_cdf("clickstream_raw_eh")
    .filter(col("_is_poison_pill") == False)
    # ── withWatermark — bound state for dropDuplicates, handle late arrivals ───
    .withWatermark("_eh_enqueued_time", "1 hour")
    # ── dropDuplicates — exactly-once on event_id within watermark window ──────
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
        col("_eh_enqueued_time"),
        # ── regexp_extract — UTM source from URL query string ─────────────────
        regexp_extract(col("page_url"), r"[?&]utm_source=([^&]+)", 1)
            .alias("utm_source"),
        # ── hour() — event hour (0-23) for intraday analysis ─────────────────
        hour(to_timestamp(col("event_time"))).alias("event_hour"),
        # ── when/otherwise — coarser device category ──────────────────────────
        when(col("device_type").isin("mobile", "tablet"), "touch")
        .when(col("device_type") == "desktop",             "desktop")
        .otherwise("unknown").alias("device_category"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("clickstream_raw_eh").alias("_bronze_source"),
    )
)

# ── stream-static join — enrich clickstream events with product dimension ──────
# stream-static joins do NOT require watermark on static side
df_clickstream_enriched = (
    df_clickstream_base
    .join(df_products_static, on="product_id", how="left")
    .select(
        df_clickstream_base["*"],
        col("product_name"),
        col("category_l1").alias("product_category_l1"),
        col("price_tier").alias("product_price_tier"),
    )
)

q_clickstream = write_stream(df_clickstream_enriched, "clickstream")
print("✅ Stream started → silver.clickstream  (enriched with product dim)")

# COMMAND ----------
# DBTITLE 1, Cell 3 — device_telemetry
# Techniques:
#   coalesce     — null-safe metric fallback to 0
#   when/otherwise — composite health_score from multiple metrics
#   dropDuplicates + withWatermark

df_telemetry = (
    read_cdf("device_telemetry_raw_eh")
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
        # ── coalesce — substitute 0 if metric is NULL rather than dropping row ─
        coalesce(col("cpu_pct").cast("double"),      lit(0.0)).alias("cpu_pct"),
        coalesce(col("memory_pct").cast("double"),   lit(0.0)).alias("memory_pct"),
        coalesce(col("disk_pct").cast("double"),     lit(0.0)).alias("disk_pct"),
        coalesce(col("temperature_c").cast("double"),lit(0.0)).alias("temperature_c"),
        col("network_in_kbps").cast("double"),
        col("network_out_kbps").cast("double"),
        col("error_count").cast("int"),
        col("uptime_seconds").cast("int"),
        col("status"),
        col("alert_triggered").cast("boolean"),
        # ── when/otherwise — composite health score 0-100 ─────────────────────
        # Lower cpu/mem/disk/temp → higher health; each contributes 25 points
        spark_round(
            (when(col("cpu_pct").cast("double")      <= 50,  25).otherwise(
             when(col("cpu_pct").cast("double")      <= 80,  15).otherwise(0)))
          + (when(col("memory_pct").cast("double")   <= 60,  25).otherwise(
             when(col("memory_pct").cast("double")   <= 85,  15).otherwise(0)))
          + (when(col("disk_pct").cast("double")     <= 70,  25).otherwise(
             when(col("disk_pct").cast("double")     <= 90,  15).otherwise(0)))
          + (when(col("temperature_c").cast("double")<= 60,  25).otherwise(
             when(col("temperature_c").cast("double")<= 85,  15).otherwise(0))),
          1
        ).alias("health_score"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("device_telemetry_raw_eh").alias("_bronze_source"),
    )
)

q_telemetry = write_stream(df_telemetry, "device_telemetry")
print("✅ Stream started → silver.device_telemetry  (health_score computed)")

# COMMAND ----------
# DBTITLE 1, Cell 4 — order_events  (foreachBatch + posexplode_outer + transform)
# Techniques:
#   foreachBatch    — custom per-microbatch logic (upsert with dedup)
#   posexplode_outer — explode line_items preserving rows with empty arrays
#   transform()     — higher-order: uppercase product_id inside array
#   date_trunc      — hour bucket for event_time

from delta.tables import DeltaTable

def process_order_events(batch_df, batch_id):
    """
    foreachBatch sink: 
    - Dedup on event_id within the microbatch
    - Write order_events header rows via MERGE (idempotent)
    - Explode and write order_event_line_items via MERGE
    """
    # ── parse and transform line_items ──────────────────────────────────────
    df_parsed = (
        batch_df
        .filter(col("_is_poison_pill") == False)
        .withWatermark("_eh_enqueued_time", "1 hour")
        .dropDuplicates(["event_id"])
        .withColumn(
            "line_items_raw",
            from_json(col("line_items"), LINE_ITEM_SCHEMA)
        )
        # ── transform() higher-order — uppercase product_id in every array element
        .withColumn(
            "line_items_parsed",
            transform(
                col("line_items_raw"),
                lambda item: item.withField(
                    "product_id", upper(item["product_id"])
                )
            )
        )
    )

    # ── order_events header ──────────────────────────────────────────────────
    df_headers = df_parsed.select(
        col("event_id"),
        col("event_type"),
        col("order_id"),
        col("customer_id"),
        col("warehouse_id"),
        to_timestamp(col("event_time")).alias("event_time"),
        to_date(col("order_date")).alias("order_date"),
        # ── date_trunc — truncate to hour for event time bucketing ────────────
        date_trunc("hour", to_timestamp(col("event_time"))).alias("event_hour_bucket"),
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
    )

    # MERGE for idempotent upsert
    DeltaTable.forName(spark, f"{SILVER}.order_events") \
        .alias("t") \
        .merge(df_headers.alias("s"), "t.event_id = s.event_id") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    # ── order_event_line_items — posexplode_outer ──────────────────────────
    # posexplode_outer preserves header rows even when line_items array is empty
    df_lines = (
        df_parsed
        .filter(col("line_items_parsed").isNotNull())
        .select(
            col("event_id"),
            col("order_id"),
            to_date(to_timestamp(col("event_time"))).alias("_event_date"),
            posexplode_outer(col("line_items_parsed")).alias("line_index", "item"),
        )
        .select(
            col("event_id"),
            col("order_id"),
            col("line_index"),
            upper(col("item.product_id")).alias("product_id"),       # already uppercased by transform()
            col("item.quantity").cast("int").alias("quantity"),
            col("item.unit_price").cast("double").alias("unit_price"),
            col("item.discount_pct").cast("double").alias("discount_pct"),
            spark_round(
                col("item.quantity").cast("double") *
                col("item.unit_price").cast("double") *
                (1 - col("item.discount_pct").cast("double") / 100), 2
            ).alias("line_total"),
            col("_event_date"),
            current_timestamp().alias("_silver_ingest_time"),
            lit("order_events_raw_eh").alias("_bronze_source"),
        )
    )

    DeltaTable.forName(spark, f"{SILVER}.order_event_line_items") \
        .alias("t") \
        .merge(
            df_lines.alias("s"),
            "t.event_id = s.event_id AND t.line_index = s.line_index"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    print(f"  Batch {batch_id}: {df_headers.count()} orders, {df_lines.count()} line items")

# ── foreachBatch — route each microbatch through custom function ───────────────
q_order_events = (
    read_cdf("order_events_raw_eh")
    .writeStream
    .foreachBatch(process_order_events)
    .option("checkpointLocation", f"{CHKPT_BASE}/order_events_fg")
    .trigger(processingTime="30 seconds")
    .start()
)

print("✅ Stream started → silver.order_events + silver.order_event_line_items  (foreachBatch)")

# COMMAND ----------
# DBTITLE 1, Cell 5 — dq_alerts
# Techniques:
#   array_filter (higher-order) — remove NULL entries from sample_failed_values
#   array_distinct              — deduplicate failure samples
#   array_compact               — drop nulls (Spark 3.4+ alias for array_filter null check)

df_dq = (
    read_cdf("dq_alerts_raw_eh")
    .withWatermark("_eh_enqueued_time", "1 hour")
    .dropDuplicates(["alert_id"])
    .withColumn(
        "sample_failed_raw",
        from_json(col("sample_failed_values"), ArrayType(StringType()))
    )
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
        # ── array_filter (higher-order) — remove NULL/empty strings from array ─
        array_filter(
            col("sample_failed_raw"),
            lambda x: x.isNotNull()
        ).alias("sample_failed_cleaned"),
        # ── array_distinct — remove duplicate failure samples in the list ──────
        array_distinct(
            array_filter(col("sample_failed_raw"), lambda x: x.isNotNull())
        ).alias("sample_failed_values"),
        # ── size — count of unique failure samples ─────────────────────────────
        size(
            array_distinct(
                array_filter(col("sample_failed_raw"), lambda x: x.isNotNull())
            )
        ).alias("unique_failure_sample_count"),
        col("action_taken"),
        col("resolution_required").cast("boolean"),
        col("runbook_url"),
        col("correlation_id"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("dq_alerts_raw_eh").alias("_bronze_source"),
    )
)

q_dq = write_stream(df_dq, "dq_alerts")
print("✅ Stream started → silver.dq_alerts  (array_filter + array_distinct)")

# COMMAND ----------
# DBTITLE 1, Cell 6 — Poison pills (3 sources → silver.poison_pills)
# Separate checkpoint per source — all write to same target table

q_cs_pills = write_stream(
    read_cdf("clickstream_raw_eh")
    .filter(col("_is_poison_pill") == True)
    .select(
        col("event_id").alias("record_id"),
        lit("clickstream").alias("source_topic"),
        lit("clickstream_raw_eh").alias("source_table"),
        to_json(col("event_id")).alias("raw_payload"),
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

print("✅ Poison pill streams started → silver.poison_pills  (3 sources)")

# COMMAND ----------
# DBTITLE 1, Cell 7 — Block (stop with ■ Interrupt)
spark.streams.awaitAnyTermination()

# COMMAND ----------
# DBTITLE 1, Cell 8 — Row counts (run after stopping)
from pyspark.sql.functions import max as spark_max

print("── Streaming Silver — Row Counts ────────────────────────────────────")
for tbl in ["clickstream", "device_telemetry", "order_events",
            "order_event_line_items", "dq_alerts", "poison_pills"]:
    df     = spark.table(f"{SILVER}.{tbl}")
    rows   = df.count()
    latest = df.select(spark_max("_silver_ingest_time")).collect()[0][0] if rows > 0 else "—"
    print(f"  {'✅' if rows > 0 else '❌'}  {tbl:<30}  {rows:>8,}  latest: {latest}")

# COMMAND ----------
# DBTITLE 1, Cell 9 — Spot checks
print("── Clickstream: UTM source distribution ────────────────────────────")
spark.table(f"{SILVER}.clickstream") \
    .groupBy("utm_source", "device_category") \
    .count().orderBy("count", ascending=False).show(8)

print("── Device telemetry: health score distribution ──────────────────────")
spark.table(f"{SILVER}.device_telemetry") \
    .groupBy(
        when(col("health_score") >= 75, "healthy")
        .when(col("health_score") >= 40, "degraded")
        .otherwise("critical").alias("health_bucket")
    ).count().show()

print("── Order events: hourly bucket distribution ─────────────────────────")
spark.table(f"{SILVER}.order_events") \
    .groupBy("event_hour_bucket") \
    .count().orderBy("event_hour_bucket").show(8)

print("── DQ alerts: unique_failure_sample_count distribution ─────────────")
spark.table(f"{SILVER}.dq_alerts") \
    .groupBy("severity", "rule_type") \
    .agg(
        spark_count("alert_id").alias("alert_count"),
        spark_avg("unique_failure_sample_count").alias("avg_unique_samples"),
    ).orderBy("alert_count", ascending=False).show()

print("── Poison pills by source ───────────────────────────────────────────")
spark.table(f"{SILVER}.poison_pills") \
    .groupBy("source_topic", "poison_reason") \
    .count().orderBy("count", ascending=False).show()
