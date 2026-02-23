# Databricks notebook source
# MAGIC %md
# MAGIC # Clickstream DLT Pipeline (Python Procedural)
# MAGIC **Pattern:** Python decorators · streaming tables · expectations · quarantine · windowed aggregation
# MAGIC **Contrast with:** supply_chain_dlt.sql (SQL declarative approach)
# MAGIC
# MAGIC Graph:
# MAGIC ```
# MAGIC Event Hub (cg-dbx-stream)
# MAGIC     └─► bronze_clickstream_stream          (STREAMING TABLE — raw ingest)
# MAGIC             ├─► bronze_clickstream_valid    (STREAMING TABLE — validated, drops bad rows)
# MAGIC             └─► bronze_clickstream_quarantine (STREAMING TABLE — captures dropped rows)
# MAGIC                     └─► silver_clickstream_deduped  (STREAMING TABLE — dedup by event_id)
# MAGIC                             ├─► silver_session_aggregates (LIVE TABLE — stateful window)
# MAGIC                             └─► silver_hourly_events     (LIVE TABLE — 1-hour rollups)
# MAGIC                                     └─► gold_wide_session_events   (LIVE TABLE — no-join wide)
# MAGIC                                     └─► gold_agg_hourly_clickstream (LIVE TABLE — dashboard)
# MAGIC ```

import dlt
import json
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, IntegerType, TimestampType,
)

# ── Pipeline config — passed from DLT pipeline settings / DABs ──────────────
CATALOG         = spark.conf.get("catalog",         "omnioc_dev")
STORAGE_ACCOUNT = spark.conf.get("storage_account", "omniocdeveusst01")
EH_NAMESPACE    = spark.conf.get("eventhub_namespace", "omnioc-dev-eus-ehns-std-01")
SECRET_SCOPE    = spark.conf.get("secret_scope",    "omnioc-kv-scope")
EH_NAME         = "eh-clickstream"
CONSUMER_GROUP  = "cg-dbx-stream"

def adls(container, path=""):
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"

# ── Clickstream JSON schema (matches omnioc_kafka_producer.py output) ────────
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

VALID_EVENT_TYPES = {
    "page_view", "search", "click", "add_to_cart",
    "remove_from_cart", "purchase", "session_start", "session_end",
}

# ════════════════════════════════════════════════════════════════════════════════
#  1 — BRONZE: Raw stream from Event Hubs
# ════════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name    = "bronze_clickstream_stream",
    comment = "Raw clickstream events from Event Hubs — binary body decoded, minimal transforms",
    table_properties = {
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
    },
)
def bronze_clickstream_stream():
    connection_string = dbutils.secrets.get(
        scope=SECRET_SCOPE, key="eventhub-clickstream-connection-string"
    )
    eh_connection = f"{connection_string};EntityPath={EH_NAME}"
    eh_conf = {
        "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs \
            .EventHubsUtils.encrypt(eh_connection),
        "eventhubs.consumerGroup": CONSUMER_GROUP,
        "eventhubs.startingPosition": json.dumps({
            "offset": "@latest", "seqNo": -1,
            "enqueuedTime": None, "isInclusive": True,
        }),
        "eventhubs.maxEventsPerTrigger": 1000,
    }
    return (
        spark.readStream
        .format("eventhubs")
        .options(**eh_conf)
        .load()
        .withColumn("body_str",         F.col("body").cast("string"))
        .withColumn("event",            F.from_json("body_str", CLICKSTREAM_SCHEMA))
        .select(
            "event.*",
            F.col("enqueuedTime").alias("eh_enqueued_time"),
            F.col("offset").cast("string").alias("eh_offset"),
            F.col("sequenceNumber").alias("eh_sequence_no"),
            F.col("partitionId").alias("eh_partition_id"),
        )
        .withColumn("event_time",      F.to_timestamp("event_time"))
        .withColumn("processing_time", F.to_timestamp("processing_time"))
        .withColumn("_ingest_time",    F.current_timestamp())
        .withColumn("_source",         F.lit(f"eventhubs/{EH_NAME}"))
        .withColumn("_parse_failed",   F.col("event_id").isNull())
    )

# ════════════════════════════════════════════════════════════════════════════════
#  2 — BRONZE: Validated (drops bad rows, routes to quarantine)
# ════════════════════════════════════════════════════════════════════════════════

@dlt.expect_or_drop("event_id_not_null",      "event_id IS NOT NULL")
@dlt.expect_or_drop("session_id_not_null",    "session_id IS NOT NULL")
@dlt.expect_or_drop("event_time_not_future",  "event_time <= current_timestamp() + INTERVAL 1 HOUR")
@dlt.expect("valid_event_type",
    f"event_type IN ('page_view','search','click','add_to_cart','remove_from_cart','purchase','session_start','session_end')")
@dlt.expect("valid_cart_value",               "cart_value IS NULL OR cart_value >= 0")
@dlt.table(
    name    = "bronze_clickstream_valid",
    comment = "Clickstream — passed DQ constraints. Rows failing DROP constraints go to quarantine.",
    table_properties = {"quality": "bronze"},
)
def bronze_clickstream_valid():
    return dlt.read_stream("bronze_clickstream_stream").filter(
        F.col("_parse_failed") == False
    )


@dlt.table(
    name    = "bronze_clickstream_quarantine",
    comment = "Clickstream events that failed DQ expectations — ops investigation table",
    table_properties = {"quality": "bronze", "delta.enableChangeDataFeed": "true"},
)
def bronze_clickstream_quarantine():
    """Captures records that would fail DROP ROW constraints — for DQ root cause analysis."""
    return (
        dlt.read_stream("bronze_clickstream_stream")
        .filter(
            F.col("_parse_failed") |
            F.col("event_id").isNull() |
            F.col("session_id").isNull() |
            F.col("_is_poison_pill")
        )
        .withColumn("_quarantine_reason", F.when(F.col("_parse_failed"), "json_parse_failed")
                                           .when(F.col("event_id").isNull(), "null_event_id")
                                           .when(F.col("session_id").isNull(), "null_session_id")
                                           .otherwise("poison_pill"))
        .withColumn("_quarantine_time", F.current_timestamp())
    )

# ════════════════════════════════════════════════════════════════════════════════
#  3 — SILVER: Deduplicated stream (event_id dedup within watermark)
# ════════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name    = "silver_clickstream_deduped",
    comment = "Clickstream — deduplicated by event_id within 10-min watermark, late arrivals handled",
    table_properties = {
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)
def silver_clickstream_deduped():
    return (
        dlt.read_stream("bronze_clickstream_valid")
        # Watermark: allow up to 10 minutes late arrivals before discarding state
        .withWatermark("event_time", "10 minutes")
        # Dedup by event_id — producer may send duplicates in retry scenarios
        .dropDuplicates(["event_id"])
        # Enrich with derived fields
        .withColumn("is_authenticated",  F.col("user_id").isNotNull())
        .withColumn("is_purchase_event", F.col("event_type") == "purchase")
        .withColumn("is_cart_event",     F.col("event_type").isin("add_to_cart","remove_from_cart"))
        .withColumn("hour_of_day",       F.hour("event_time"))
        .withColumn("day_of_week",       F.dayofweek("event_time"))
        .withColumn("_silver_time",      F.current_timestamp())
    )

# ════════════════════════════════════════════════════════════════════════════════
#  4 — SILVER: Windowed session aggregates (stateful, 30-min tumbling windows)
# ════════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name    = "silver_session_aggregates",
    comment = "Session-level aggregates: page views, cart additions, purchases per 30-min window",
    table_properties = {"quality": "silver"},
)
def silver_session_aggregates():
    return (
        dlt.read_stream("silver_clickstream_deduped")
        .withWatermark("event_time", "15 minutes")
        .groupBy(
            F.window("event_time", "30 minutes"),
            "session_id",
            "user_id",
            "device_type",
            "geo_country",
            "geo_city",
        )
        .agg(
            F.count("*")                              .alias("total_events"),
            F.sum(F.col("event_type") == "page_view") .alias("page_views"),
            F.sum(F.col("event_type") == "search")    .alias("searches"),
            F.sum(F.col("event_type") == "add_to_cart").alias("cart_adds"),
            F.sum(F.col("event_type") == "purchase")  .alias("purchases"),
            F.max("cart_value")                       .alias("max_cart_value"),
            F.sum("purchase_amount")                  .alias("total_purchase_amount"),
            F.min("event_time")                       .alias("session_start"),
            F.max("event_time")                       .alias("session_end"),
            F.max("session_page_count")               .alias("max_page_count"),
            F.collect_set("category")                 .alias("categories_viewed"),
        )
        .withColumn("session_duration_minutes",
            (F.col("session_end").cast("long") - F.col("session_start").cast("long")) / 60)
        .withColumn("converted", F.col("purchases") > 0)
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )

# ════════════════════════════════════════════════════════════════════════════════
#  5 — GOLD: Wide session events (no-join Big Table design)
# ════════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name    = "gold_wide_session_events",
    comment = "Gold Big Table: all session KPIs in one wide row — no-join funnel analysis",
    table_properties = {"quality": "gold"},
)
def gold_wide_session_events():
    return (
        dlt.read("silver_session_aggregates")
        .withColumn("conversion_rate",
            F.when(F.col("total_events") > 0,
                F.col("purchases") / F.col("total_events") * 100
            ).otherwise(0.0))
        .withColumn("engagement_score",
            # Weighted score: page_views*1 + searches*2 + cart_adds*3 + purchases*10
            F.col("page_views") * 1 +
            F.col("searches")   * 2 +
            F.col("cart_adds")  * 3 +
            F.col("purchases")  * 10
        )
        .withColumn("_gold_updated_at", F.current_timestamp())
    )

# ════════════════════════════════════════════════════════════════════════════════
#  6 — GOLD: Hourly clickstream aggregate (dashboard feed)
# ════════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name    = "gold_agg_hourly_clickstream",
    comment = "Gold aggregate: hourly clickstream KPIs by country/category/device — dashboard feed",
    table_properties = {"quality": "gold"},
)
def gold_agg_hourly_clickstream():
    return (
        dlt.read_stream("silver_clickstream_deduped")
        .withWatermark("event_time", "15 minutes")
        .groupBy(
            F.window("event_time", "1 hour"),
            "geo_country",
            "device_type",
            "category",
        )
        .agg(
            F.count("*")                               .alias("total_events"),
            F.countDistinct("session_id")              .alias("unique_sessions"),
            F.countDistinct("user_id")                 .alias("unique_users"),
            F.sum(F.col("event_type") == "purchase")   .alias("purchases"),
            F.sum("purchase_amount")                   .alias("total_revenue"),
            F.avg("cart_value")                        .alias("avg_cart_value"),
        )
        .withColumn("hour",  F.col("window.start"))
        .drop("window")
        .withColumn("_computed_at", F.current_timestamp())
    )
