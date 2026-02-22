# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Catalog Setup — Run This First
# MAGIC Creates omnioc_dev catalog with bronze/silver/gold schemas.
# MAGIC Run ONCE before any other bronze notebook.

# COMMAND ----------
# DBTITLE 1, Create Catalog with Managed Location
spark.sql("""
    CREATE CATALOG IF NOT EXISTS omnioc_dev
    MANAGED LOCATION 'abfss://ucroot@omniocdeveusst01.dfs.core.windows.net/omnioc_dev'
""")
print("✅ Catalog omnioc_dev created")

# COMMAND ----------
# DBTITLE 1, Create Schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS omnioc_dev.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS omnioc_dev.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS omnioc_dev.gold")
print("✅ Schemas created: bronze, silver, gold")

# COMMAND ----------
# DBTITLE 1, Verify
spark.sql("SHOW SCHEMAS IN omnioc_dev").show()

# COMMAND ----------
# DBTITLE 1, Verify ADLS Access
files = dbutils.fs.ls("abfss://landing@omniocdeveusst01.dfs.core.windows.net/")
print("✅ ADLS landing/ accessible — folders found:")
for f in files:
    print(f"   {f.path}")

# COMMAND ----------
# DBTITLE 1, Verify Secret Scope
conn = dbutils.secrets.get('omnioc-kv-scope', 'eventhub-clickstream-connection-string')
print("✅ Secret scope working — eventhub-clickstream-connection-string resolved")
print("\n🎯 All checks passed — ready to run notebooks 01 → 05")
