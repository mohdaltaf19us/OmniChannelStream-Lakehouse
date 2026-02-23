# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Big Table (No-Join Serving Layer)
# MAGIC **Pattern:** Pre-joined, pre-aggregated wide rows. Sub-second dashboard queries with no joins at read time.
# MAGIC **Tables:** wide_customer_order_summary · wide_product_performance · agg_daily_sales_by_region

# COMMAND ----------
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

dbutils.widgets.text("catalog", "omnioc_dev", "Unity Catalog")
CATALOG = dbutils.widgets.get("catalog")

def gold(t):   return f"{CATALOG}.gold.{t}"
def bronze(t): return f"{CATALOG}.bronze.{t}"
def silver(t): return f"{CATALOG}.silver.{t}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

# COMMAND ----------
# MAGIC %md ## 1 — wide_customer_order_summary
# MAGIC All customer KPIs in one row. Powers customer 360 dashboard, cohort analysis, churn ML features.

print("Building wide_customer_order_summary...")

customers = spark.table(bronze("customers_raw"))
orders    = spark.table(bronze("orders_raw")) \
    .withColumn("order_date", F.to_timestamp("order_date"))
payments  = spark.table(bronze("payments_raw")) \
    .filter(F.col("status") == "completed")
refunds   = spark.table(bronze("refunds_raw")) \
    if spark.catalog.tableExists(bronze("refunds_raw")) \
    else spark.createDataFrame([], orders.schema)

# Order aggregates per customer
order_agg = (
    orders
    .groupBy("customer_id")
    .agg(
        F.count("*")                                                     .alias("total_orders"),
        F.sum("total_amount")                                            .alias("gross_revenue"),
        F.sum(F.col("total_amount") - F.coalesce("discount_amount",F.lit(0))).alias("net_revenue"),
        F.avg("total_amount")                                            .alias("avg_order_value"),
        F.sum("discount_amount")                                         .alias("total_discounts"),
        F.min("order_date")                                              .alias("first_order_date"),
        F.max("order_date")                                              .alias("last_order_date"),
        F.count(F.when(F.col("status") == "delivered", 1))              .alias("delivered_orders"),
        F.count(F.when(F.col("status") == "cancelled", 1))              .alias("cancelled_orders"),
        F.count(F.when(F.col("status") == "returned",  1))              .alias("returned_orders"),
        F.count(F.when(F.col("channel") == "mobile_app", 1))            .alias("mobile_orders"),
        F.count(F.when(F.col("channel") == "web", 1))                   .alias("web_orders"),
        F.count(F.when(F.col("promo_code").isNotNull(), 1))              .alias("promo_orders"),
        F.countDistinct("currency")                                      .alias("currencies_used"),
        F.countDistinct("shipping_address_country")                      .alias("ship_countries"),
    )
)

# Payment aggregates
pay_agg = (
    payments.groupBy("order_id")
    .agg(F.sum("amount").alias("total_paid"))
    .join(orders.select("order_id", "customer_id"), "order_id")
    .groupBy("customer_id")
    .agg(F.sum("total_paid").alias("total_revenue_paid"))
)

wide_customer = (
    customers
    .join(order_agg, "customer_id", "left")
    .join(pay_agg,   "customer_id", "left")
    .withColumn("days_since_first_order",
        F.datediff(F.current_date(), F.to_date("first_order_date")))
    .withColumn("days_since_last_order",
        F.datediff(F.current_date(), F.to_date("last_order_date")))
    .withColumn("customer_lifetime_days",
        F.datediff(F.current_date(), F.to_date("signup_date")))
    .withColumn("order_frequency_per_month",
        F.when(F.col("customer_lifetime_days") > 0,
            F.col("total_orders") / (F.col("customer_lifetime_days") / 30.0)
        ).otherwise(0.0))
    .withColumn("churn_risk_score",
        # Simple rule-based score (ML feature candidate)
        F.when(F.col("days_since_last_order") > 180, 0.9)
         .when(F.col("days_since_last_order") > 90,  0.6)
         .when(F.col("days_since_last_order") > 30,  0.3)
         .otherwise(0.1))
    .withColumn("customer_value_segment",
        F.when(F.col("net_revenue") >= 5000, "High Value")
         .when(F.col("net_revenue") >= 1000, "Mid Value")
         .when(F.col("net_revenue") > 0,     "Low Value")
         .otherwise("No Purchases"))
    .withColumn("cancellation_rate",
        F.when(F.col("total_orders") > 0,
            F.col("cancelled_orders") / F.col("total_orders") * 100).otherwise(0.0))
    .withColumn("return_rate",
        F.when(F.col("total_orders") > 0,
            F.col("returned_orders") / F.col("total_orders") * 100).otherwise(0.0))
    .withColumn("_computed_at", F.current_timestamp())
    .select(
        # Customer identity
        "customer_id","first_name","last_name","email","segment",
        "country","city","state","loyalty_points",
        # Order KPIs
        F.coalesce("total_orders", F.lit(0)).alias("total_orders"),
        F.coalesce("gross_revenue", F.lit(0.0)).alias("gross_revenue"),
        F.coalesce("net_revenue", F.lit(0.0)).alias("net_revenue"),
        F.coalesce("total_revenue_paid", F.lit(0.0)).alias("total_revenue_paid"),
        F.coalesce("avg_order_value", F.lit(0.0)).alias("avg_order_value"),
        F.coalesce("total_discounts", F.lit(0.0)).alias("total_discounts"),
        "first_order_date","last_order_date",
        F.coalesce("delivered_orders", F.lit(0)).alias("delivered_orders"),
        F.coalesce("cancelled_orders", F.lit(0)).alias("cancelled_orders"),
        F.coalesce("returned_orders",  F.lit(0)).alias("returned_orders"),
        F.coalesce("mobile_orders", F.lit(0)).alias("mobile_orders"),
        F.coalesce("web_orders",    F.lit(0)).alias("web_orders"),
        F.coalesce("promo_orders",  F.lit(0)).alias("promo_orders"),
        # Derived metrics
        "days_since_first_order","days_since_last_order",
        "customer_lifetime_days","order_frequency_per_month",
        "churn_risk_score","customer_value_segment",
        "cancellation_rate","return_rate",
        "_computed_at",
    )
)

wide_customer.write.format("delta").mode("overwrite").saveAsTable(gold("wide_customer_order_summary"))
print(f"✅ wide_customer_order_summary: {spark.table(gold('wide_customer_order_summary')).count():,} rows")

# COMMAND ----------
# MAGIC %md ## 2 — wide_product_performance

print("Building wide_product_performance...")

products   = spark.table(bronze("products_raw"))
order_lines= spark.table(bronze("order_lines_raw")) \
    if spark.catalog.tableExists(bronze("order_lines_raw")) \
    else spark.createDataFrame([], schema="line_id string, order_id string, product_id string, quantity int, unit_price double, discount_pct double, line_total double")

orders_for_prod = spark.table(bronze("orders_raw")) \
    .select("order_id","order_date","customer_id","status","channel") \
    .withColumn("order_date", F.to_timestamp("order_date"))

line_with_order = order_lines.join(orders_for_prod, "order_id", "left")

product_agg = (
    line_with_order
    .groupBy("product_id")
    .agg(
        F.sum("quantity")                                          .alias("total_units_sold"),
        F.sum("line_total")                                        .alias("total_revenue"),
        F.avg("line_total")                                        .alias("avg_line_value"),
        F.avg("discount_pct")                                      .alias("avg_discount_pct"),
        F.countDistinct("order_id")                                .alias("orders_containing"),
        F.countDistinct("customer_id")                             .alias("unique_customers"),
        F.count(F.when(F.col("status") == "returned", 1))         .alias("returned_line_count"),
        F.min("order_date")                                        .alias("first_sold_date"),
        F.max("order_date")                                        .alias("last_sold_date"),
        F.count(F.when(F.col("channel") == "mobile_app", 1))      .alias("mobile_app_sales"),
        F.count(F.when(F.col("channel") == "marketplace", 1))     .alias("marketplace_sales"),
    )
)

wide_product = (
    products
    .join(product_agg, "product_id", "left")
    .withColumn("margin_pct",
        F.when(F.col("unit_price") > 0,
            (F.col("unit_price") - F.col("unit_cost")) / F.col("unit_price") * 100
        ).otherwise(0.0))
    .withColumn("total_margin",
        F.coalesce("total_units_sold", F.lit(0)) * (F.col("unit_price") - F.col("unit_cost")))
    .withColumn("return_rate_pct",
        F.when(F.col("orders_containing") > 0,
            F.col("returned_line_count") / F.col("orders_containing") * 100
        ).otherwise(0.0))
    .withColumn("days_since_last_sale",
        F.datediff(F.current_date(), F.to_date("last_sold_date")))
    .withColumn("velocity_band",
        F.when(F.coalesce("total_units_sold", F.lit(0)) > 5000, "Fast Moving")
         .when(F.coalesce("total_units_sold", F.lit(0)) > 500,  "Medium Moving")
         .when(F.coalesce("total_units_sold", F.lit(0)) > 0,    "Slow Moving")
         .otherwise("No Sales"))
    .withColumn("_computed_at", F.current_timestamp())
    .select(
        "product_id","product_name","sku","category","sub_category","supplier_id",
        "unit_cost","unit_price","margin_pct","weight_kg","is_active",
        F.coalesce("total_units_sold",    F.lit(0))  .alias("total_units_sold"),
        F.coalesce("total_revenue",       F.lit(0.0)).alias("total_revenue"),
        F.coalesce("total_margin",        F.lit(0.0)).alias("total_margin"),
        F.coalesce("avg_line_value",      F.lit(0.0)).alias("avg_line_value"),
        F.coalesce("avg_discount_pct",    F.lit(0.0)).alias("avg_discount_pct"),
        F.coalesce("orders_containing",   F.lit(0))  .alias("orders_containing"),
        F.coalesce("unique_customers",    F.lit(0))  .alias("unique_customers"),
        F.coalesce("returned_line_count", F.lit(0))  .alias("returned_line_count"),
        "first_sold_date","last_sold_date",
        F.coalesce("mobile_app_sales",    F.lit(0))  .alias("mobile_app_sales"),
        F.coalesce("marketplace_sales",   F.lit(0))  .alias("marketplace_sales"),
        "return_rate_pct","days_since_last_sale","velocity_band","_computed_at",
    )
)

wide_product.write.format("delta").mode("overwrite").saveAsTable(gold("wide_product_performance"))
print(f"✅ wide_product_performance: {spark.table(gold('wide_product_performance')).count():,} rows")

# COMMAND ----------
# MAGIC %md ## 3 — agg_daily_sales_by_region (Finance dashboard)

print("Building agg_daily_sales_by_region...")

daily_agg = (
    spark.table(bronze("orders_raw"))
    .withColumn("order_date", F.to_date(F.to_timestamp("order_date")))
    .join(
        spark.table(bronze("warehouses_raw")).select("warehouse_id","region"),
        "warehouse_id", "left"
    )
    .groupBy("order_date","region","currency","channel")
    .agg(
        F.count("*")                     .alias("order_count"),
        F.sum("total_amount")            .alias("gross_revenue"),
        F.sum("discount_amount")         .alias("total_discounts"),
        F.sum("tax_amount")              .alias("total_tax"),
        F.sum("shipping_amount")         .alias("total_shipping"),
        F.avg("total_amount")            .alias("avg_order_value"),
        F.countDistinct("customer_id")   .alias("unique_customers"),
        F.count(F.when(F.col("status")=="delivered", 1)).alias("delivered"),
        F.count(F.when(F.col("status")=="cancelled", 1)).alias("cancelled"),
        F.count(F.when(F.col("promo_code").isNotNull(), 1)).alias("promo_orders"),
    )
    .withColumn("net_revenue", F.col("gross_revenue") - F.coalesce("total_discounts",F.lit(0)))
    .withColumn("_computed_at", F.current_timestamp())
)

daily_agg.write.format("delta").mode("overwrite").saveAsTable(gold("agg_daily_sales_by_region"))
print(f"✅ agg_daily_sales_by_region: {spark.table(gold('agg_daily_sales_by_region')).count():,} rows")
print("\n✅ All Big Table / aggregate tables built")
