# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming: Clickstream — Standalone Structured Streaming
# MAGIC Used when NOT running DLT. Same Event Hubs source, writes directly to Bronze Delta.
# MAGIC Pattern: foreachBatch for exactly-once MERGE (upsert) semantics.

# COMMAND ----------
import json
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import *
from delta.tables import DeltaTable

dbutils.widgets.text("catalog",          "omnioc_dev",       "Unity Catalog")
dbutils.widgets.text("storage_account",  "omniocdeveusst01", "ADLS Account")
dbutils.widgets.text("secret_scope",     "omnioc-kv-scope",  "Secret Scope")
dbutils.widgets.text("trigger_interval", "30 seconds",       "Trigger interval")

CATALOG         = dbutils.widgets.get("catalog")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
SECRET_SCOPE    = dbutils.widgets.get("secret_scope")
TRIGGER_INT     = dbutils.widgets.get("trigger_interval")
EH_NAME         = "eh-clickstream"
CONSUMER_GROUP  = "cg-dbx-stream"

def adls(c, p=""): return f"abfss://{c}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{p}"

BRONZE_TABLE    = f"{CATALOG}.bronze.clickstream_raw"
SILVER_TABLE    = f"{CATALOG}.silver.clickstream_deduped"
CHECKPOINT_B    = adls("checkpoint", "streaming/clickstream_bronze/")
CHECKPOINT_S    = adls("checkpoint", "streaming/clickstream_silver/")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")

# COMMAND ----------
# MAGIC %md ## Schema

SCHEMA = StructType([
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
    StructField("geo_country",        StringType(),  True),
    StructField("geo_city",           StringType(),  True),
    StructField("session_page_count", IntegerType(), True),
    StructField("_is_late_arrival",   BooleanType(), True),
    StructField("_is_poison_pill",    BooleanType(), True),
])

# COMMAND ----------
# MAGIC %md ## Event Hubs config

conn_str = dbutils.secrets.get(scope=SECRET_SCOPE, key="eventhub-clickstream-connection-string")
eh_conf  = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs \
        .EventHubsUtils.encrypt(f"{conn_str};EntityPath={EH_NAME}"),
    "eventhubs.consumerGroup": CONSUMER_GROUP,
    "eventhubs.startingPosition": json.dumps({
        "offset": "@latest", "seqNo": -1, "enqueuedTime": None, "isInclusive": True
    }),
    "eventhubs.maxEventsPerTrigger": 500,
}

# COMMAND ----------
# MAGIC %md ## Raw stream → Bronze (append)

raw = (
    spark.readStream.format("eventhubs").options(**eh_conf).load()
    .withColumn("body_str",     F.col("body").cast("string"))
    .withColumn("event",        F.from_json("body_str", SCHEMA))
    .select("event.*",
            F.col("enqueuedTime").alias("eh_enqueued_time"),
            F.col("sequenceNumber").alias("eh_sequence_no"),
            F.col("partitionId").alias("eh_partition_id"))
    .withColumn("event_time",      F.to_timestamp("event_time"))
    .withColumn("processing_time", F.to_timestamp("processing_time"))
    .withColumn("_ingest_time",    F.current_timestamp())
    .withColumn("_source",         F.lit(f"eventhubs/{EH_NAME}"))
    .withColumn("_parse_failed",   F.col("event_id").isNull())
)

q_bronze = (
    raw.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(processingTime=TRIGGER_INT)
    .option("checkpointLocation", CHECKPOINT_B)
    .option("mergeSchema", "true")
    .toTable(BRONZE_TABLE)
)
print(f"✅ Bronze stream: {BRONZE_TABLE}")

# COMMAND ----------
# MAGIC %md ## Bronze → Silver (foreachBatch MERGE — exactly once dedup)

def upsert_to_silver(batch_df: DataFrame, batch_id: int):
    """
    Exactly-once upsert using foreachBatch + MERGE.
    Deduplicates within the batch first, then merges into Silver.
    This pattern guarantees exactly-once even if a micro-batch replays.
    """
    if batch_df.rdd.isEmpty():
        return

    # Step 1: Dedup within this micro-batch by event_id
    deduped = (
        batch_df
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("_is_poison_pill") == False)
        .filter(F.col("event_time") <= F.current_timestamp() + F.expr("INTERVAL 1 HOUR"))
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["event_id"])
        .withColumn("is_authenticated",  F.col("user_id").isNotNull())
        .withColumn("is_purchase_event", F.col("event_type") == "purchase")
        .withColumn("hour_of_day",       F.hour("event_time"))
        .withColumn("_silver_time",      F.current_timestamp())
    )

    # Step 2: MERGE into Silver Delta table (upsert — update if event_id exists)
    if spark.catalog.tableExists(SILVER_TABLE):
        silver = DeltaTable.forName(spark, SILVER_TABLE)
        (
            silver.alias("target")
            .merge(
                deduped.alias("source"),
                "target.event_id = source.event_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # First run — create the table
        deduped.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)

    print(f"  Batch {batch_id}: merged {deduped.count():,} rows into {SILVER_TABLE}")

q_silver = (
    raw.filter(F.col("_parse_failed") == False)
    .writeStream
    .foreachBatch(upsert_to_silver)
    .trigger(processingTime=TRIGGER_INT)
    .option("checkpointLocation", CHECKPOINT_S)
    .start()
)
print(f"✅ Silver stream: {SILVER_TABLE}")
print(f"\n⚡ Both streams running. Trigger: {TRIGGER_INT}")
print(f"   q_bronze.stop() and q_silver.stop() to terminate")
