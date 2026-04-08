# Phase 5 – Databricks Olist Pipeline

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, to_date, when
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, dense_rank


orders = spark.table("workspace.default.olist_orders_dataset")
customers = spark.table("workspace.default.olist_customers_dataset")
order_items = spark.table("workspace.default.olist_order_items_dataset")
payments = spark.table("workspace.default.olist_order_payments_dataset")
products = spark.table("workspace.default.olist_products_dataset")
sellers = spark.table("workspace.default.olist_sellers_dataset")
reviews = spark.table("workspace.default.olist_order_reviews_dataset")
geo = spark.table("workspace.default.olist_geolocation_dataset")
category = spark.table("workspace.default.product_category_name_translation")

display(orders)
display(customers)
display(order_items)
orders.printSchema()


orders.select([count(when(col(c).isNull(), c)).alias(c) for c in orders.columns]).show()
print('Orders:', orders.count())
print('Customers:', customers.count())

fact_orders = order_items.join(orders, 'order_id').join(customers, 'customer_id').join(payments, 'order_id')
display(fact_orders)

# ============================
# 🔹 TASK 1: Top 3 Customers per City
# ============================

customer_orders = customers.join(orders, "customer_id") \
    .join(payments, "order_id")

customer_spend = customer_orders.groupBy("customer_city", "customer_id") \
    .agg(sum("payment_value").alias("total_spend"))

window_city = Window.partitionBy("customer_city").orderBy(col("total_spend").desc())

top_customers = customer_spend.withColumn("rank", row_number().over(window_city)) \
    .filter(col("rank") <= 3)

top_customers.show()

# ============================
# 🔹 TASK 2: Running Total Sales
# ============================

daily_sales = payments.join(orders, "order_id") \
    .withColumn("date", to_date("order_purchase_timestamp")) \
    .groupBy("date").agg(sum("payment_value").alias("daily_sales"))

window_date = Window.orderBy("date")

running_sales = daily_sales.withColumn("running_total", sum("daily_sales").over(window_date))

running_sales.show()

# ============================
# 🔹 TASK 3: Top Products per Category
# ============================

from pyspark.sql.functions import sum, col, dense_rank
from pyspark.sql.window import Window

# Join (optional, but not needed if no category)
product_sales = order_items.groupBy("product_id") \
    .agg(sum("price").alias("total_sales"))

# Global ranking
window_all = Window.orderBy(col("total_sales").desc())

top_products = product_sales.withColumn("rank", dense_rank().over(window_all))

top_products.show()

# ============================
# 🔹 TASK 4: Customer Lifetime Value
# ============================

clv = payments.groupBy("order_id").agg(sum("payment_value").alias("total_spend"))

clv.show()

# ============================
# 🔹 TASK 5: Customer Segmentation
# ============================

from pyspark.sql.functions import col, when

# Customer Lifetime Value (CLV)
clv = order_items.join(orders, "order_id") \
                 .groupBy("customer_id") \
                 .agg(sum("price").alias("total_spend"))

# Segmentation
seg = clv.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when(col("total_spend") >= 5000, "Silver")
    .otherwise("Bronze")
)

# Count customers per segment
segment_count = seg.groupBy("segment").count()


display(segment_count)
# ============================
# 🔹 TASK 6: Final Reporting Table
# ============================

from pyspark.sql.functions import count

# Count total orders per customer
orders_count = orders.groupBy("customer_id") \
                     .agg(count("order_id").alias("total_orders"))

# Final report
final = seg.join(customers, "customer_id") \
           .join(orders_count, "customer_id") \
           .select(
               "customer_id",
               "customer_city",
               "total_spend",
               "segment",
               "total_orders"
           )

display(final)
print("🎉 Phase 5 Completed Successfully")
