# Databricks notebook source
# MAGIC %md
# MAGIC # Kafka Connection Test — EH Kafka Endpoint
# MAGIC Run cells **one at a time, top to bottom.**
# MAGIC Each cell is independent — if one fails, fix before moving to the next.
# MAGIC
# MAGIC | Cell | What it tests |
# MAGIC |------|--------------|
# MAGIC | 1 | Build JAAS config from Key Vault secret |
# MAGIC | 2 | Read 5 messages from `clickstream-events` |
# MAGIC | 3 | Read 5 messages from `device-telemetry` |
# MAGIC | 4 | Read 5 messages from `order-events` |
# MAGIC | 5 | Read 5 messages from `dq-alerts` |
# MAGIC | 6 | Write 1 test message to `eh-clickstream` |
# MAGIC | 7 | Confirm test message arrived |

# COMMAND ----------
# DBTITLE 1, Cell 1 — Build JAAS config
# Pull namespace-level connection string from Key Vault
NS_CONN_STR = dbutils.secrets.get("omnioc-kv-scope", "eventhub-clickstream-connection-string")

# Strip EntityPath if present — Kafka needs namespace-level string only
if "EntityPath" in NS_CONN_STR:
    NS_CONN_STR = ";".join(p for p in NS_CONN_STR.split(";") if not p.startswith("EntityPath"))

EH_BOOTSTRAP = "omnioc-dev-eus-ehns-std-01.servicebus.windows.net:9093"

# DBR 15.4 — standard class, NOT kafkashaded prefix
JAAS = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="$ConnectionString" password="{NS_CONN_STR}";'
)

# Common options reused in every read cell
KAFKA_OPTS = {
    "kafka.bootstrap.servers"  : EH_BOOTSTRAP,
    "kafka.security.protocol"  : "SASL_SSL",
    "kafka.sasl.mechanism"     : "PLAIN",
    "kafka.sasl.jaas.config"   : JAAS,
    "kafka.request.timeout.ms" : "60000",
    "kafka.session.timeout.ms" : "30000",
    "startingOffsets"          : "earliest",
    "endingOffsets"            : "latest",    # batch read — stop at latest offset
    "failOnDataLoss"           : "false",
}

print("✅ JAAS config built")
print(f"   Bootstrap : {EH_BOOTSTRAP}")
print(f"   Class     : org.apache.kafka.common.security.plain.PlainLoginModule")
print(f"   Username  : $ConnectionString")
print(f"   Password  : [secret — {len(NS_CONN_STR)} chars]")

# COMMAND ----------
# DBTITLE 1, Cell 2 — Read from clickstream-events
print("── Testing: clickstream-events ─────────────────────────────────────")

try:
    df = (
        spark.read
        .format("kafka")
        .options(**KAFKA_OPTS)
        .option("subscribe", "clickstream-events")
        .load()
    )

    total = df.count()
    print(f"✅ Messages found : {total:,}")

    if total > 0:
        print(f"\n  Sample — last 5 messages (raw Kafka envelope):")
        df.select(
            "partition",
            "offset",
            "timestamp",
            df["value"].cast("string").alias("value_preview")
        ).orderBy("timestamp", ascending=False).limit(5).show(truncate=80)
    else:
        print("⚠️  0 messages — topic may be empty or retention expired")

except Exception as e:
    print(f"❌ FAILED: {e}")

# COMMAND ----------
# DBTITLE 1, Cell 3 — Read from device-telemetry
print("── Testing: device-telemetry ───────────────────────────────────────")

try:
    df = (
        spark.read
        .format("kafka")
        .options(**KAFKA_OPTS)
        .option("subscribe", "device-telemetry")
        .load()
    )

    total = df.count()
    print(f"✅ Messages found : {total:,}")

    if total > 0:
        print(f"\n  Sample — last 5 messages:")
        df.select(
            "partition",
            "offset",
            "timestamp",
            df["value"].cast("string").alias("value_preview")
        ).orderBy("timestamp", ascending=False).limit(5).show(truncate=80)
    else:
        print("⚠️  0 messages — topic may be empty or retention expired")

except Exception as e:
    print(f"❌ FAILED: {e}")

# COMMAND ----------
# DBTITLE 1, Cell 4 — Read from order-events
print("── Testing: order-events ───────────────────────────────────────────")

try:
    df = (
        spark.read
        .format("kafka")
        .options(**KAFKA_OPTS)
        .option("subscribe", "order-events")
        .load()
    )

    total = df.count()
    print(f"✅ Messages found : {total:,}")

    if total > 0:
        print(f"\n  Sample — last 5 messages:")
        df.select(
            "partition",
            "offset",
            "timestamp",
            df["value"].cast("string").alias("value_preview")
        ).orderBy("timestamp", ascending=False).limit(5).show(truncate=80)
    else:
        print("⚠️  0 messages — topic may be empty or retention expired")

except Exception as e:
    print(f"❌ FAILED: {e}")

# COMMAND ----------
# DBTITLE 1, Cell 5 — Read from dq-alerts
print("── Testing: dq-alerts ──────────────────────────────────────────────")

try:
    df = (
        spark.read
        .format("kafka")
        .options(**KAFKA_OPTS)
        .option("subscribe", "dq-alerts")
        .load()
    )

    total = df.count()
    print(f"✅ Messages found : {total:,}")

    if total > 0:
        print(f"\n  Sample — last 5 messages:")
        df.select(
            "partition",
            "offset",
            "timestamp",
            df["value"].cast("string").alias("value_preview")
        ).orderBy("timestamp", ascending=False).limit(5).show(truncate=80)
    else:
        print("⚠️  0 messages — topic may be empty or retention expired")

except Exception as e:
    print(f"❌ FAILED: {e}")

# COMMAND ----------
# DBTITLE 1, Cell 6 — Write 1 test message to eh-clickstream
import json
from pyspark.sql.functions import lit, to_json, struct

print("── Testing WRITE: eh-clickstream ───────────────────────────────────")

test_payload = json.dumps({
    "event_id"   : "TEST-KAFKA-WRITE-001",
    "event_type" : "test_connection",
    "source"     : "databricks_kafka_write_test",
    "cluster"    : spark.conf.get("spark.databricks.clusterUsageTags.clusterName", "unknown"),
})

try:
    test_df = spark.createDataFrame(
        [(test_payload,)], ["value"]
    )

    (
        test_df.write
        .format("kafka")
        .options(**{k: v for k, v in KAFKA_OPTS.items()
                    if k not in ("startingOffsets", "endingOffsets")})
        .option("topic", "eh-clickstream")
        .save()
    )

    print("✅ Test message written to eh-clickstream")
    print(f"   Payload : {test_payload}")
    print("\n  Run Cell 7 to confirm it arrived")

except Exception as e:
    print(f"❌ FAILED: {e}")

# COMMAND ----------
# DBTITLE 1, Cell 7 — Confirm test message arrived
print("── Confirming test message in eh-clickstream ───────────────────────")

try:
    df_check = (
        spark.read
        .format("kafka")
        .options(**KAFKA_OPTS)
        .option("subscribe", "eh-clickstream")
        .load()
    )

    from pyspark.sql.functions import col

    # Look for our specific test message
    test_msgs = (
        df_check
        .filter(col("value").cast("string").contains("TEST-KAFKA-WRITE-001"))
    )

    found = test_msgs.count()

    if found > 0:
        print(f"✅ Test message confirmed in eh-clickstream")
        test_msgs.select(
            "partition", "offset", "timestamp",
            col("value").cast("string").alias("value")
        ).show(truncate=120)
    else:
        total = df_check.count()
        print(f"⚠️  Test message not found — {total:,} total messages in topic")
        print("   Either Cell 6 failed or retention has already expired")

except Exception as e:
    print(f"❌ FAILED: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Reading Results
# MAGIC
# MAGIC | Result | Meaning | Fix |
# MAGIC |--------|---------|-----|
# MAGIC | ✅ Messages found | Kafka read works | Move to EH native test |
# MAGIC | ❌ `TimeoutException` | JAAS class wrong or network issue | Check `org.apache.kafka` class (not `kafkashaded`) |
# MAGIC | ❌ `SaslAuthenticationException` | Wrong connection string or Key Vault secret | Check `eventhub-clickstream-connection-string` in KV |
# MAGIC | ❌ `UnknownTopicOrPartitionException` | Topic name mismatch | Check exact hub names in portal |
# MAGIC | ⚠️ 0 messages | Topic empty or 2h retention expired | Re-run Kafka producer then mirror script |
