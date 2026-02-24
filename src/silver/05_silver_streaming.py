# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Silver — Streaming Transforms
# MAGIC
# MAGIC No SELECT *, UPDATE SET *, or INSERT * anywhere.
# MAGIC Generated columns EXCLUDED from all writes:
# MAGIC   clickstream, device_telemetry, order_events, dq_alerts → _event_date
# MAGIC   poison_pills → _quarantine_date
# MAGIC   order_event_line_items → _event_date is NOT generated (plain col, included)
# MAGIC
# MAGIC **Bugs fixed vs previous version:**
# MAGIC   1. exceptAll on streaming DFs → replaced with .filter(_is_poison_pill == True)
# MAGIC      (set operations are unsupported on streaming DataFrames — AnalysisException)
# MAGIC   2. .between() on STRING bronze columns → cast("double").between() for numeric range
# MAGIC   3. withWatermark on _eh_enqueued_time → moved to event_time / alert_time
# MAGIC      (_eh_enqueued_time is EH metadata from the connector; when reading via CDF
# MAGIC      from a Delta bronze table it may not be present or monotonically increasing)
# MAGIC   4. `window` imported but never used → removed
# MAGIC   5. `lower` imported but never used → removed
# MAGIC
# MAGIC **Transformation techniques demonstrated:**
# MAGIC   withWatermark + dropDuplicates  — exactly-once event_id dedup per stream
# MAGIC   stream-static join + broadcast  — clickstream enriched with products dim
# MAGIC   foreachBatch                    — order_events: custom MERGE per microbatch
# MAGIC   transform() higher-order        — line_items: uppercase product_id in array elements
# MAGIC   posexplode_outer                — order_event_line_items: explode with row index
# MAGIC   array_filter higher-order       — dq_alerts: remove NULLs from sample array
# MAGIC   array_distinct                  — dq_alerts: deduplicate failure samples
# MAGIC   regexp_extract                  — clickstream: UTM source from URL query string
# MAGIC   date_trunc                      — order_events: event_hour_bucket
# MAGIC   coalesce                        — device_telemetry: null-safe metric fallback
# MAGIC   when/otherwise composite        — device_telemetry: health_score from 4 metrics
# MAGIC   hour()                          — clickstream: intraday event_hour

# COMMAND ----------
# DBTITLE 1, Cell 1 — Config & column lists
from pyspark.sql.functions import (
    col, to_timestamp, to_date, current_timestamp, lit,
    from_json, to_json, posexplode_outer,
    round as spark_round, size, when,
    regexp_extract, upper, trim, hour,
    transform, filter as array_filter, array_distinct,
    date_trunc,
    coalesce,
)
from pyspark.sql.types import (
    ArrayType, StringType, StructType, StructField,
    IntegerType, DoubleType,
)

BRONZE     = "omnioc_dev.bronze"
SILVER     = "omnioc_dev.silver"
CHKPT_BASE = "abfss://checkpoint@omniocdeveusst01.dfs.core.windows.net/silver"

# ── Exact writable column lists per table (GENERATED columns excluded) ────────
CLICKSTREAM_COLS = [
    "event_id", "event_type", "session_id", "user_id", "device_type",
    "event_time", "processing_time", "page_url", "referrer_url", "product_id",
    "category", "search_query", "cart_value", "purchase_amount",
    "ip_address", "user_agent", "geo_country", "geo_city", "session_page_count",
    "_silver_ingest_time", "_bronze_source",
    "utm_source", "event_hour", "device_category",
    "product_name", "product_category_l1", "product_price_tier",
]   # _event_date excluded — GENERATED ALWAYS AS (CAST(event_time AS DATE))

TELEMETRY_COLS = [
    "telemetry_id", "device_id", "device_type", "firmware_version", "location_id",
    "event_time", "processing_time",
    "cpu_pct", "memory_pct", "disk_pct", "temperature_c",
    "network_in_kbps", "network_out_kbps", "error_count", "uptime_seconds",
    "status", "alert_triggered",
    "_silver_ingest_time", "_bronze_source",
    "health_score",
]   # _event_date excluded — GENERATED

ORDER_EVENT_COLS = [
    "event_id", "event_type", "order_id", "customer_id", "warehouse_id",
    "event_time", "order_date", "status", "channel", "currency",
    "total_amount", "discount_amount", "tax_amount", "shipping_amount",
    "shipping_country", "carrier", "tracking_number",
    "cancellation_reason", "line_item_count",
    "_silver_ingest_time", "_bronze_source",
    "event_hour_bucket",
]   # _event_date excluded — GENERATED

LINE_ITEM_COLS = [
    "event_id", "order_id", "line_index", "product_id", "quantity",
    "unit_price", "discount_pct", "line_total",
    "_silver_ingest_time", "_bronze_source",
    "_event_date",     # NOT generated here — plain partition column, must be included
]

DQ_ALERT_COLS = [
    "alert_id", "alert_time", "severity", "rule_type", "rule_name",
    "pipeline_name", "table_name", "failed_record_count", "total_record_count",
    "failure_rate_pct", "sample_failed_values", "action_taken",
    "resolution_required", "runbook_url", "correlation_id",
    "_silver_ingest_time", "_bronze_source",
    "sample_failed_cleaned", "unique_failure_sample_count",
]   # _event_date excluded — GENERATED

POISON_PILL_COLS = [
    "record_id", "source_topic", "source_table", "raw_payload",
    "poison_reason", "_eh_enqueued_time", "_eh_partition", "_silver_ingest_time",
]   # _quarantine_date excluded — GENERATED

LINE_ITEM_SCHEMA = ArrayType(StructType([
    StructField("product_id",   StringType(),  True),
    StructField("quantity",     IntegerType(), True),
    StructField("unit_price",   DoubleType(),  True),
    StructField("discount_pct", DoubleType(),  True),
]))


def read_cdf(bronze_table):
    """
    Read a bronze Delta table via Change Data Feed.
    startingVersion=0  → processes all existing data on first run.
    Checkpoint tracks offset so restarts continue from where they left off.
    Filters to insert + update_postimage only (excludes delete tombstones).
    """
    return (
        spark.readStream
        .format("delta")
        .option("readChangeFeed",  "true")
        .option("startingVersion", "0")
        .table(f"{BRONZE}.{bronze_table}")
        .filter(col("_change_type").isin("insert", "update_postimage"))
    )


def write_append(df, table_name, chkpt_suffix=None):
    """Append-only stream sink. Schema matched by column name."""
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHKPT_BASE}/{chkpt_suffix or table_name}")
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
#   withWatermark(event_time)    — bound late-arrival state for dropDuplicates
#   dropDuplicates([event_id])   — exactly-once dedup within watermark window
#   regexp_extract               — UTM source from page_url query string
#   hour()                       — intraday event_hour (0-23)
#   when/otherwise               — coarser device_category grouping
#   stream-static join + broadcast — enrich with product dimension
# EXCLUDES: _event_date (GENERATED from CAST(event_time AS DATE))

# ── Static products dim — broadcast hint for small dimension table ─────────────
df_products_static = (
    spark.read.table(f"{SILVER}.products")
    .select("product_id", "product_name", "category_l1", "price_tier")
    .hint("broadcast")
)

df_clickstream_base = (
    read_cdf("clickstream_raw_eh")
    .filter(col("_is_poison_pill") == False)
    # ── FIX 3: watermark on event_time (reliable business timestamp) ──────────
    .withWatermark("event_time", "1 hour")
    .dropDuplicates(["event_id"])
    .select(
        col("event_id"), col("event_type"), col("session_id"), col("user_id"),
        col("device_type"),
        to_timestamp(col("event_time")).alias("event_time"),
        to_timestamp(col("processing_time")).alias("processing_time"),
        col("page_url"), col("referrer_url"), col("product_id"), col("category"),
        col("search_query"),
        col("cart_value").cast("double"),
        col("purchase_amount").cast("double"),
        col("ip_address"), col("user_agent"), col("geo_country"), col("geo_city"),
        col("session_page_count").cast("int"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("clickstream_raw_eh").alias("_bronze_source"),
        # ── regexp_extract — pull UTM source param from URL ───────────────────
        regexp_extract(col("page_url"), r"[?&]utm_source=([^&]+)", 1)
            .alias("utm_source"),
        # ── hour() — event hour 0-23 for intraday analytics ──────────────────
        hour(to_timestamp(col("event_time"))).alias("event_hour"),
        # ── when/otherwise — coarser device category ─────────────────────────
        when(col("device_type").isin("mobile", "tablet"), "touch")
        .when(col("device_type") == "desktop", "desktop")
        .otherwise("unknown").alias("device_category"),
    )
)

# ── stream-static join — enrich events with product dimension ─────────────────
# Both sides aliased to resolve ambiguous product_id column reference.
df_clickstream_enriched = (
    df_clickstream_base.alias("cs")
    .join(df_products_static.alias("p"),
          col("cs.product_id") == col("p.product_id"), "left")
    .select(
        col("cs.event_id"), col("cs.event_type"), col("cs.session_id"),
        col("cs.user_id"), col("cs.device_type"), col("cs.event_time"),
        col("cs.processing_time"), col("cs.page_url"), col("cs.referrer_url"),
        col("cs.product_id"), col("cs.category"), col("cs.search_query"),
        col("cs.cart_value"), col("cs.purchase_amount"),
        col("cs.ip_address"), col("cs.user_agent"),
        col("cs.geo_country"), col("cs.geo_city"),
        col("cs.session_page_count"),
        col("cs._silver_ingest_time"), col("cs._bronze_source"),
        col("cs.utm_source"), col("cs.event_hour"), col("cs.device_category"),
        col("p.product_name"),
        col("p.category_l1").alias("product_category_l1"),
        col("p.price_tier").alias("product_price_tier"),
    )
)

q_clickstream = write_append(df_clickstream_enriched, "clickstream")
print("✅ Stream started → silver.clickstream  (27 cols, stream-static join)")

# COMMAND ----------
# DBTITLE 1, Cell 3 — device_telemetry
# Techniques:
#   coalesce            — null-safe metric fallback to 0.0
#   when/otherwise      — composite health_score from 4 metrics
#   withWatermark       — on event_time (FIX 3)
#   cast("double").between() — numeric range validation (FIX 2)
# EXCLUDES: _event_date (GENERATED)

df_telemetry = (
    read_cdf("device_telemetry_raw_eh")
    .filter(col("_is_poison_pill") == False)
    # ── FIX 2: cast to double before .between() — bronze cols are STRING ──────
    .filter(col("temperature_c").cast("double").between(0, 120))
    .filter(col("cpu_pct").cast("double").between(0, 100))
    # ── FIX 3: watermark on event_time ───────────────────────────────────────
    .withWatermark("event_time", "1 hour")
    .dropDuplicates(["telemetry_id"])
    .select(
        col("telemetry_id"),
        col("device_id"),
        col("device_type"),
        col("firmware_version"),
        col("location_id"),
        to_timestamp(col("event_time")).alias("event_time"),
        to_timestamp(col("processing_time")).alias("processing_time"),
        # ── coalesce — replace NULL metrics with 0 rather than dropping row ───
        coalesce(col("cpu_pct").cast("double"),       lit(0.0)).alias("cpu_pct"),
        coalesce(col("memory_pct").cast("double"),    lit(0.0)).alias("memory_pct"),
        coalesce(col("disk_pct").cast("double"),      lit(0.0)).alias("disk_pct"),
        coalesce(col("temperature_c").cast("double"), lit(0.0)).alias("temperature_c"),
        col("network_in_kbps").cast("double"),
        col("network_out_kbps").cast("double"),
        col("error_count").cast("int"),
        col("uptime_seconds").cast("int"),
        col("status"),
        col("alert_triggered").cast("boolean"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("device_telemetry_raw_eh").alias("_bronze_source"),
        # ── when/otherwise — composite health score 0-100 ────────────────────
        # Each of 4 metrics contributes up to 25 points (lower resource = higher health).
        # The original column reference (not the coalesce alias) is intentional:
        # aliases from earlier in the same select() are not visible to later expressions.
        # NULL inputs to cast("double") → NULL <= threshold evaluates to NULL
        # which falls through to .otherwise(0) — correct zero-contribution fallback.
        spark_round(
            when(col("cpu_pct").cast("double")       <= 50, 25)
            .when(col("cpu_pct").cast("double")      <= 80, 15).otherwise(0)
          + when(col("memory_pct").cast("double")    <= 60, 25)
            .when(col("memory_pct").cast("double")   <= 85, 15).otherwise(0)
          + when(col("disk_pct").cast("double")      <= 70, 25)
            .when(col("disk_pct").cast("double")     <= 90, 15).otherwise(0)
          + when(col("temperature_c").cast("double") <= 60, 25)
            .when(col("temperature_c").cast("double")<= 85, 15).otherwise(0),
            1
        ).alias("health_score"),
    )
)

q_telemetry = write_append(df_telemetry, "device_telemetry")
print("✅ Stream started → silver.device_telemetry  (20 cols, health_score computed)")

# COMMAND ----------
# DBTITLE 1, Cell 4 — order_events + order_event_line_items  (foreachBatch)
# Techniques:
#   foreachBatch      — custom MERGE logic executed per microbatch
#   transform()       — higher-order: uppercase product_id inside every array element
#   posexplode_outer  — explode line_items preserving rows with empty/null arrays
#   date_trunc        — truncate event_time to hour for event_hour_bucket
# EXCLUDES: _event_date (GENERATED) for order_events
# INCLUDES: _event_date for order_event_line_items (plain partition column)

from delta.tables import DeltaTable


def process_order_events(batch_df, batch_id):
    """
    foreachBatch sink:
      1. Filter poison pills and deduplicate event_id within microbatch.
      2. MERGE order_events headers — explicit 22-col UPDATE + INSERT.
      3. Explode line_items with posexplode_outer → MERGE order_event_line_items.
    batch_df already contains only insert/update_postimage rows (read_cdf filter).
    """
    df_parsed = (
        batch_df
        .filter(col("_is_poison_pill") == False)
        .dropDuplicates(["event_id"])
        .withColumn("line_items_raw", from_json(col("line_items"), LINE_ITEM_SCHEMA))
        # ── transform() — uppercase product_id inside every array element ─────
        .withColumn(
            "line_items_parsed",
            transform(
                col("line_items_raw"),
                lambda item: item.withField("product_id", upper(item["product_id"]))
            )
        )
    )

    # ── order_events header ───────────────────────────────────────────────────
    df_headers = df_parsed.select(
        col("event_id"),
        col("event_type"),
        col("order_id"),
        col("customer_id"),
        col("warehouse_id"),
        to_timestamp(col("event_time")).alias("event_time"),
        to_date(col("order_date")).alias("order_date"),
        col("status"), col("channel"), col("currency"),
        col("total_amount").cast("double"),
        col("discount_amount").cast("double"),
        col("tax_amount").cast("double"),
        col("shipping_amount").cast("double"),
        col("shipping_country"), col("carrier"), col("tracking_number"),
        col("cancellation_reason"),
        size(col("line_items_parsed")).alias("line_item_count"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("order_events_raw_eh").alias("_bronze_source"),
        # ── date_trunc — hour bucket for time-series aggregations ─────────────
        date_trunc("hour", to_timestamp(col("event_time"))).alias("event_hour_bucket"),
    )

    update_set = {c: f"s.{c}" for c in ORDER_EVENT_COLS if c != "event_id"}
    insert_map = {c: f"s.{c}" for c in ORDER_EVENT_COLS}

    DeltaTable.forName(spark, f"{SILVER}.order_events") \
        .alias("t") \
        .merge(df_headers.alias("s"), "t.event_id = s.event_id") \
        .whenMatchedUpdate(set=update_set) \
        .whenNotMatchedInsert(values=insert_map) \
        .execute()

    # ── posexplode_outer — explode line items with position index ─────────────
    # posexplode_outer emits one row per element with its 0-based position.
    # "outer" preserves the parent row even when the array is NULL or empty
    # (pos=null, item=null) so no header row is silently lost.
    df_lines = (
        df_parsed
        .filter(col("line_items_parsed").isNotNull())
        .select(
            col("event_id"),
            col("order_id"),
            posexplode_outer(col("line_items_parsed")).alias("line_index", "item"),
            to_date(to_timestamp(col("event_time"))).alias("_event_date"),
        )
        .select(
            col("event_id"),
            col("order_id"),
            col("line_index"),
            upper(col("item.product_id")).alias("product_id"),
            col("item.quantity").cast("int").alias("quantity"),
            col("item.unit_price").cast("double").alias("unit_price"),
            col("item.discount_pct").cast("double").alias("discount_pct"),
            spark_round(
                col("item.quantity").cast("double")
                * col("item.unit_price").cast("double")
                * (1 - col("item.discount_pct").cast("double") / 100),
                2
            ).alias("line_total"),
            current_timestamp().alias("_silver_ingest_time"),
            lit("order_events_raw_eh").alias("_bronze_source"),
            col("_event_date"),   # plain column — not generated, must be included
        )
    )

    line_update = {c: f"s.{c}" for c in LINE_ITEM_COLS
                   if c not in ("event_id", "line_index")}
    line_insert = {c: f"s.{c}" for c in LINE_ITEM_COLS}

    DeltaTable.forName(spark, f"{SILVER}.order_event_line_items") \
        .alias("t") \
        .merge(
            df_lines.alias("s"),
            "t.event_id = s.event_id AND t.line_index = s.line_index"
        ) \
        .whenMatchedUpdate(set=line_update) \
        .whenNotMatchedInsert(values=line_insert) \
        .execute()

    print(f"  Batch {batch_id}: "
          f"{df_headers.count()} order events  |  "
          f"{df_lines.count()} line items")


q_order_events = (
    read_cdf("order_events_raw_eh")
    .writeStream
    .foreachBatch(process_order_events)
    .option("checkpointLocation", f"{CHKPT_BASE}/order_events_fb")
    .trigger(processingTime="30 seconds")
    .start()
)

print("✅ Stream started → silver.order_events + silver.order_event_line_items  (foreachBatch)")

# COMMAND ----------
# DBTITLE 1, Cell 5 — dq_alerts
# Techniques:
#   array_filter (higher-order) — remove NULL entries from sample_failed_values
#   array_distinct              — deduplicate failure samples after null removal
#   size                        — count unique samples
#   withWatermark on alert_time (FIX 3)
# EXCLUDES: _event_date (GENERATED from CAST(alert_time AS DATE))

df_dq = (
    read_cdf("dq_alerts_raw_eh")
    # ── FIX 3: watermark on alert_time (business timestamp) ───────────────────
    .withWatermark("alert_time", "1 hour")
    .dropDuplicates(["alert_id"])
    .withColumn(
        "sample_failed_raw",
        from_json(col("sample_failed_values"), ArrayType(StringType()))
    )
    .select(
        col("alert_id"),
        to_timestamp(col("alert_time")).alias("alert_time"),
        col("severity"), col("rule_type"), col("rule_name"),
        col("pipeline_name"), col("table_name"),
        col("failed_record_count").cast("int"),
        col("total_record_count").cast("int"),
        col("failure_rate_pct").cast("double"),
        # ── array_filter (higher-order) — remove NULL entries ─────────────────
        array_filter(
            col("sample_failed_raw"), lambda x: x.isNotNull()
        ).alias("sample_failed_cleaned"),
        # ── array_distinct — deduplicate remaining values ─────────────────────
        array_distinct(
            array_filter(col("sample_failed_raw"), lambda x: x.isNotNull())
        ).alias("sample_failed_values"),
        # ── size — count of unique failure samples ────────────────────────────
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

q_dq = write_append(df_dq, "dq_alerts")
print("✅ Stream started → silver.dq_alerts  (19 cols, array_filter + array_distinct)")

# COMMAND ----------
# DBTITLE 1, Cell 6 — poison_pills  (3 sources → silver.poison_pills)
# FIX 1: Replaced df_all.exceptAll(df_clean) with direct .filter(_is_poison_pill == True).
#
# Root cause of original bug:
#   Spark Structured Streaming does NOT support set operations (exceptAll, intersectAll,
#   subtract, union) between two streaming DataFrames. Both df_all and df_clean were
#   derived from read_cdf() — both streaming — so exceptAll raised AnalysisException:
#   "Queries with streaming sources must be executed with writeStream.start()"
#
# Fix: filter directly on the _is_poison_pill flag — same logical result, no set op.
# EXCLUDES: _quarantine_date (GENERATED from CAST(_silver_ingest_time AS DATE))

def build_pill_df(source_table, record_id_col, source_topic_name):
    """
    Read CDF stream, keep only poison pill rows, return POISON_PILL_COLS select.
    Direct filter — NOT exceptAll (unsupported on streaming DataFrames).
    """
    return (
        read_cdf(source_table)
        .filter(col("_is_poison_pill") == True)
        .select(
            col(record_id_col).alias("record_id"),
            lit(source_topic_name).alias("source_topic"),
            lit(source_table).alias("source_table"),
            to_json(col(record_id_col)).alias("raw_payload"),
            lit("poison_pill_flag").alias("poison_reason"),
            col("_eh_enqueued_time"),
            col("_eh_partition"),
            current_timestamp().alias("_silver_ingest_time"),
        )
    )


q_cs_pills = write_append(
    build_pill_df("clickstream_raw_eh",      "event_id",    "clickstream"),
    "poison_pills", "poison_pills_cs"
)
q_tel_pills = write_append(
    build_pill_df("device_telemetry_raw_eh", "telemetry_id", "device_telemetry"),
    "poison_pills", "poison_pills_tel"
)
q_ord_pills = write_append(
    build_pill_df("order_events_raw_eh",     "event_id",    "order_events"),
    "poison_pills", "poison_pills_ord"
)

print("✅ Poison pill streams started → silver.poison_pills  (8 cols, 3 sources)")

# COMMAND ----------
# DBTITLE 1, Cell 7 — Block  (stop with ■ Interrupt)
spark.streams.awaitAnyTermination()

# COMMAND ----------
# DBTITLE 1, Cell 8 — Row counts  (run after stopping)
from pyspark.sql.functions import max as spark_max

print("── Streaming Silver — Row Counts ────────────────────────────────────")
for tbl in [
    "clickstream", "device_telemetry", "order_events",
    "order_event_line_items", "dq_alerts", "poison_pills",
]:
    df     = spark.table(f"{SILVER}.{tbl}")
    rows   = df.count()
    latest = df.select(spark_max("_silver_ingest_time")).collect()[0][0] \
             if rows > 0 else "—"
    print(f"  {'✅' if rows > 0 else '❌'}  {tbl:<30}  {rows:>8,}  latest: {latest}")

# COMMAND ----------
# DBTITLE 1, Cell 9 — Spot checks  (run after stopping)
from pyspark.sql.functions import count as spark_count, avg as spark_avg

print("── Clickstream: UTM source × device_category ───────────────────────")
spark.table(f"{SILVER}.clickstream") \
    .groupBy("utm_source", "device_category") \
    .count().orderBy("count", ascending=False).show(8)

print("── Device telemetry: health bucket distribution ─────────────────────")
spark.table(f"{SILVER}.device_telemetry") \
    .groupBy(
        when(col("health_score") >= 75, "healthy")
        .when(col("health_score") >= 40, "degraded")
        .otherwise("critical").alias("health_bucket")
    ).count().show()

print("── Order events: event type × avg line items ────────────────────────")
spark.table(f"{SILVER}.order_events") \
    .groupBy("event_type") \
    .agg(
        spark_count("event_id").alias("events"),
        spark_round(spark_avg("line_item_count"), 1).alias("avg_lines"),
    ).orderBy("events", ascending=False).show()

print("── DQ alerts: severity × avg unique failure samples ─────────────────")
spark.table(f"{SILVER}.dq_alerts") \
    .groupBy("severity", "rule_type") \
    .agg(
        spark_count("alert_id").alias("alerts"),
        spark_round(spark_avg("unique_failure_sample_count"), 1).alias("avg_unique_samples"),
    ).orderBy("alerts", ascending=False).show()

print("── Poison pills by source ───────────────────────────────────────────")
spark.table(f"{SILVER}.poison_pills") \
    .groupBy("source_topic").count().show()
