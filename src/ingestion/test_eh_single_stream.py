# Databricks notebook source
# MAGIC %md
# MAGIC # EH Single Stream Test — eh-clickstream only
# MAGIC Run **one cell at a time**. Cell 1 diagnoses the connection string.
# MAGIC Cell 2 reads. Cell 3 streams.

# COMMAND ----------
# DBTITLE 1, Cell 1 — Diagnose and build clean connection string
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.types import *

raw = dbutils.secrets.get("omnioc-kv-scope", "eventhub-clickstream-connection-string")

# ── Step 1: show what keys are in the secret ─────────────────────────────────
print("── What is in the Key Vault secret? ───────────────────────────────")
for i, p in enumerate(raw.split(";")):
    if p.strip():
        key = p.strip().split("=")[0]
        print(f"  part[{i}] → '{key}'")

# ── Step 2: strip EntityPath + empty parts ───────────────────────────────────
clean = ";".join(
    p for p in raw.rstrip(";").split(";")
    if p.strip() and not p.strip().startswith("EntityPath")
)

print("\n── Clean string keys (EntityPath removed) ─────────────────────────")
for i, p in enumerate(clean.split(";")):
    key = p.strip().split("=")[0]
    print(f"  part[{i}] → '{key}'")

# ── Step 3: append EntityPath for eh-clickstream ─────────────────────────────
final = clean + ";EntityPath=eh-clickstream"

print("\n── Final string keys (EntityPath appended) ─────────────────────────")
for i, p in enumerate(final.split(";")):
    key = p.strip().split("=")[0]
    print(f"  part[{i}] → '{key}'")
print("  Expected: Endpoint, SharedAccessKeyName, SharedAccessKey, EntityPath")

# ── Step 4: encrypt ──────────────────────────────────────────────────────────
ENCRYPTED = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(final)
print("\n✅ encrypt() — OK")

# ── Step 5: build config ─────────────────────────────────────────────────────
EH_CONF = {
    "eventhubs.connectionString"    : ENCRYPTED,
    "eventhubs.consumerGroup"       : "cg-dbx-stream",
    "eventhubs.startingPosition"    : '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}',
    "eventhubs.maxEventsPerTrigger" : 100,
}

print("✅ EH_CONF built — run Cell 2")

# COMMAND ----------
# DBTITLE 1, Cell 2 — Batch read (confirms messages exist before streaming)
print("── Batch read: eh-clickstream ──────────────────────────────────────")

df = spark.read.format("eventhubs").options(**EH_CONF).load()

print(f"  Messages found : {df.count():,}")

df.printSchema()

df.select(
    "partition",
    "offset",
    "sequenceNumber",
    "enqueuedTime",
    col("body").cast("string").alias("body")
).orderBy("enqueuedTime", ascending=False).limit(3).show(truncate=100)

# COMMAND ----------
# DBTITLE 1, Cell 3 — Stream read (15 seconds into memory table, then stop)
import time

CHKPT = "abfss://checkpoint@omniocdeveusst01.dfs.core.windows.net/test/clickstream_eh_test"

CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id",           StringType(),  True),
    StructField("event_type",         StringType(),  True),
    StructField("session_id",         StringType(),  True),
    StructField("user_id",            StringType(),  True),
    StructField("device_type",        StringType(),  True),
    StructField("event_time",         StringType(),  True),
    StructField("cart_value",         DoubleType(),  True),
    StructField("purchase_amount",    DoubleType(),  True),
    StructField("session_page_count", IntegerType(), True),
    StructField("_is_late_arrival",   BooleanType(), True),
    StructField("_is_poison_pill",    BooleanType(), True),
])

q = (
    spark.readStream
    .format("eventhubs")
    .options(**EH_CONF)
    .load()
    .select(
        from_json(col("body").cast("string"), CLICKSTREAM_SCHEMA).alias("d"),
        col("enqueuedTime").alias("_eh_enqueued_time"),
        col("partition").cast("string").alias("_eh_partition"),
    )
    .select(
        "d.*",
        "_eh_enqueued_time",
        "_eh_partition",
        current_timestamp().alias("_ingest_time"),
        lit("eventhubs_native").alias("_source_system"),
    )
    .writeStream
    .format("memory")
    .queryName("cs_eh_test")
    .outputMode("append")
    .option("checkpointLocation", CHKPT)
    .trigger(processingTime="5 seconds")
    .start()
)

print("Stream started — waiting 15 seconds...")
time.sleep(15)

result = spark.sql("SELECT * FROM cs_eh_test")
print(f"Rows landed : {result.count():,}")

result.select(
    "event_id", "event_type", "user_id", "cart_value",
    "_is_late_arrival", "_eh_partition", "_ingest_time"
).show(5, truncate=60)

q.stop()
print("Stream stopped")
