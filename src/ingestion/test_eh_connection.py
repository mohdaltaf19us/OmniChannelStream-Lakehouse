# Databricks notebook source
# MAGIC %md
# MAGIC # Event Hubs Native Connection Test
# MAGIC Run cells **one at a time, top to bottom.**
# MAGIC
# MAGIC | Cell | What it tests |
# MAGIC |------|--------------|
# MAGIC | 1 | Build EH config + encrypt connection string |
# MAGIC | 2 | Read 5 messages from `eh-clickstream` |
# MAGIC | 3 | Read 5 messages from `device-telemetry` |
# MAGIC | 4 | Read 5 messages from `order-events` |
# MAGIC | 5 | Read 5 messages from `dq-alerts` |
# MAGIC | 6 | Write 1 test message via EH native sender |
# MAGIC | 7 | Confirm test message arrived |

# COMMAND ----------
# DBTITLE 1, Cell 1 — Build EH config and encrypt connection string
from pyspark.sql.types import TimestampType, LongType, StringType
from pyspark.sql.functions import col

# Pull namespace-level connection string from Key Vault
NS_CONN_STR = dbutils.secrets.get("omnioc-kv-scope", "eventhub-clickstream-connection-string")

EH_NAMESPACE = "omnioc-dev-eus-ehns-std-01"
CG           = "cg-dbx-stream"

def eh_conf(hub_name, from_beginning=True):
    """
    Build EH Spark connector config per hub.
    Appends EntityPath to namespace-level string — required by EventHubsUtils.encrypt().
    """
    conn      = NS_CONN_STR.rstrip(";") + f";EntityPath={hub_name}"
    encrypted = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn)

    starting = (
        '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}'  # beginning
        if from_beginning else
        '{"offset":"@latest","seqNo":-1,"enqueuedTime":null,"isInclusive":false}'  # latest
    )

    return {
        "eventhubs.connectionString"    : encrypted,
        "eventhubs.consumerGroup"       : CG,
        "eventhubs.maxEventsPerTrigger" : 100,
        "eventhubs.startingPosition"    : starting,
    }

# Quick sanity check — encrypt should not throw
try:
    test_encrypted = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        NS_CONN_STR.rstrip(";") + ";EntityPath=eh-clickstream"
    )
    print("✅ EventHubsUtils.encrypt()  — OK")
except Exception as e:
    print(f"❌ EventHubsUtils.encrypt() failed: {e}")
    print("   Check: Maven library com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22 installed on cluster?")

print(f"✅ Namespace  : {EH_NAMESPACE}")
print(f"✅ Consumer   : {CG}")
print("✅ Config ready — run Cell 2")

# COMMAND ----------
# DBTITLE 1, Cell 2 — Read from eh-clickstream (batch, beginning → latest)
print("── Testing READ: eh-clickstream ────────────────────────────────────")

try:
    df = (
        spark.read                               # batch read — not streaming
        .format("eventhubs")
        .options(**eh_conf("eh-clickstream"))
        .load()
    )

    total = df.count()
    print(f"✅ Messages found : {total:,}")

    if total > 0:
        print(f"\n  Schema (EH envelope):")
        df.printSchema()

        print(f"\n  Sample — last 5 messages (raw envelope):")
        df.select(
            "partition",
            "offset",
            "sequenceNumber",
            "enqueuedTime",
            col("body").cast("string").alias("body_preview")
        ).orderBy("enqueuedTime", ascending=False).limit(5).show(truncate=80)
    else:
        print("⚠️  0 messages — eh-clickstream has 2h retention, messages may have expired")
        print("   Re-run your Kafka producer + mirror script to repopulate")

except Exception as e:
    print(f"❌ FAILED: {e}")

# COMMAND ----------
# DBTITLE 1, Cell 3 — Read from device-telemetry
print("── Testing READ: device-telemetry ──────────────────────────────────")

try:
    df = (
        spark.read
        .format("eventhubs")
        .options(**eh_conf("device-telemetry"))
        .load()
    )

    total = df.count()
    print(f"✅ Messages found : {total:,}")

    if total > 0:
        df.select(
            "partition", "offset", "sequenceNumber", "enqueuedTime",
            col("body").cast("string").alias("body_preview")
        ).orderBy("enqueuedTime", ascending=False).limit(5).show(truncate=80)
    else:
        print("⚠️  0 messages — 168h retention, messages should still be there")
        print("   Check: did your mirror script run successfully?")

except Exception as e:
    print(f"❌ FAILED: {e}")

# COMMAND ----------
# DBTITLE 1, Cell 4 — Read from order-events
print("── Testing READ: order-events ──────────────────────────────────────")

try:
    df = (
        spark.read
        .format("eventhubs")
        .options(**eh_conf("order-events"))
        .load()
    )

    total = df.count()
    print(f"✅ Messages found : {total:,}")

    if total > 0:
        df.select(
            "partition", "offset", "sequenceNumber", "enqueuedTime",
            col("body").cast("string").alias("body_preview")
        ).orderBy("enqueuedTime", ascending=False).limit(5).show(truncate=80)
    else:
        print("⚠️  0 messages — 168h retention, messages should still be there")

except Exception as e:
    print(f"❌ FAILED: {e}")

# COMMAND ----------
# DBTITLE 1, Cell 5 — Read from dq-alerts
print("── Testing READ: dq-alerts ─────────────────────────────────────────")

try:
    df = (
        spark.read
        .format("eventhubs")
        .options(**eh_conf("dq-alerts"))
        .load()
    )

    total = df.count()
    print(f"✅ Messages found : {total:,}")

    if total > 0:
        df.select(
            "partition", "offset", "sequenceNumber", "enqueuedTime",
            col("body").cast("string").alias("body_preview")
        ).orderBy("enqueuedTime", ascending=False).limit(5).show(truncate=80)
    else:
        print("⚠️  0 messages — 168h retention, messages should still be there")

except Exception as e:
    print(f"❌ FAILED: {e}")

# COMMAND ----------
# DBTITLE 1, Cell 6 — Write 1 test message to eh-clickstream via EH native sender
import json

print("── Testing WRITE: eh-clickstream ───────────────────────────────────")

test_payload = json.dumps({
    "event_id"   : "TEST-EH-NATIVE-WRITE-001",
    "event_type" : "test_connection",
    "source"     : "databricks_eh_native_write_test",
}).encode("utf-8")

try:
    # EH native write — uses the Spark EventHubs connector
    conn_with_entity = NS_CONN_STR.rstrip(";") + ";EntityPath=eh-clickstream"
    encrypted        = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn_with_entity)

    write_conf = {"eventhubs.connectionString": encrypted}

    test_df = spark.createDataFrame([(test_payload,)], ["body"])

    (
        test_df.write
        .format("eventhubs")
        .options(**write_conf)
        .save()
    )

    print("✅ Test message written to eh-clickstream via EH native connector")
    print(f"   Payload : {test_payload.decode()}")
    print("\n  Run Cell 7 to confirm it arrived")

except Exception as e:
    print(f"❌ FAILED: {e}")
    print("\n  NOTE: Write requires a Shared Access Policy with Send permission.")
    print("  Your dbx-listen-policy has Listen only — that is expected.")
    print("  This cell is for completeness; reads (Cells 2–5) are what matter for Databricks.")

# COMMAND ----------
# DBTITLE 1, Cell 7 — Confirm test message arrived
print("── Confirming test message in eh-clickstream ───────────────────────")

try:
    df_check = (
        spark.read
        .format("eventhubs")
        .options(**eh_conf("eh-clickstream"))
        .load()
    )

    test_msgs = df_check.filter(
        col("body").cast("string").contains("TEST-EH-NATIVE-WRITE-001")
    )

    found = test_msgs.count()

    if found > 0:
        print(f"✅ Test message confirmed in eh-clickstream")
        test_msgs.select(
            "partition", "offset", "sequenceNumber", "enqueuedTime",
            col("body").cast("string").alias("body")
        ).show(truncate=120)
    else:
        total = df_check.count()
        print(f"⚠️  Test message not found (total messages in hub: {total:,})")
        print("   Cell 6 likely failed due to Listen-only policy — that is fine.")
        print("   If Cell 2–5 passed, your read path is working correctly.")

except Exception as e:
    print(f"❌ FAILED: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Reading Results
# MAGIC
# MAGIC | Result | Meaning | Fix |
# MAGIC |--------|---------|-----|
# MAGIC | ✅ Messages found | EH native read works | Ready to run `01_bronze_streaming_eh.py` |
# MAGIC | ❌ `encrypt() failed` | Maven library not installed | Add `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22` to cluster |
# MAGIC | ❌ `IllegalConnectionStringFormatException` | EntityPath missing or malformed | Check `eh_conf()` helper — EntityPath is appended automatically |
# MAGIC | ❌ `AuthorizationFailedException` | Wrong key or policy | Check Key Vault secret `eventhub-clickstream-connection-string` |
# MAGIC | ⚠️ 0 messages on eh-clickstream | 2h retention expired | Re-run producer + mirror script |
# MAGIC | ⚠️ 0 messages on device-telemetry / order-events / dq-alerts | Unexpected — 168h retention | Check if mirror script ran correctly |
# MAGIC | ❌ Cell 6 Write failed | Listen-only policy | Expected — `dbx-listen-policy` has no Send permission. Reads still work. |
