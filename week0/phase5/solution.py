from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, to_date
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, dense_rank

spark = SparkSession.builder.appName("Phase5").getOrCreate()

# Load datasets
customers = spark.read.csv('/FileStore/olist/olist_customers_dataset.csv', header=True, inferSchema=True)
orders = spark.read.csv('/FileStore/olist/olist_orders_dataset.csv', header=True, inferSchema=True)
payments = spark.read.csv('/FileStore/olist/olist_order_payments_dataset.csv', header=True, inferSchema=True)
products = spark.read.csv('/FileStore/olist/olist_products_dataset.csv', header=True, inferSchema=True)
items = spark.read.csv('/FileStore/olist/olist_order_items_dataset.csv', header=True, inferSchema=True)

# ---------------- TASK 1 ----------------
customer_orders = customers.join(orders, "customer_id") \
    .join(payments, "order_id")

customer_spend = customer_orders.groupBy("customer_city", "customer_id") \
    .agg(sum("payment_value").alias("total_spend"))

window_city = Window.partitionBy("customer_city").orderBy(col("total_spend").desc())

top_customers = customer_spend.withColumn("rank", row_number().over(window_city)) \
    .filter(col("rank") <= 3)

top_customers.show()

# ---------------- TASK 2 ----------------
daily_sales = payments.join(orders, "order_id") \
    .withColumn("date", to_date("order_purchase_timestamp")) \
    .groupBy("date").agg(sum("payment_value").alias("daily_sales"))

window_date = Window.orderBy("date")

running_sales = daily_sales.withColumn("running_total", sum("daily_sales").over(window_date))

running_sales.show()

# ---------------- TASK 3 ----------------
product_sales = items.join(products, "product_id") \
    .groupBy("product_category_name", "product_id") \
    .agg(sum("price").alias("total_sales"))

window_cat = Window.partitionBy("product_category_name").orderBy(col("total_sales").desc())

top_products = product_sales.withColumn("rank", dense_rank().over(window_cat))

top_products.show()

# ---------------- TASK 4 ----------------
clv = payments.groupBy("order_id").agg(sum("payment_value").alias("total_spend"))

clv.show()

# ---------------- TASK 5 ----------------
customer_total = customer_orders.groupBy("customer_id") \
    .agg(sum("payment_value").alias("total_spend"))

segmented = customer_total.withColumn(
    "segment",
    (col("total_spend") > 10000).cast("string")
)

segmented.show()

# ---------------- TASK 6 ----------------
final_df = customer_orders.groupBy("customer_id", "customer_city") \
    .agg(
        sum("payment_value").alias("total_spend"),
        count("order_id").alias("total_orders")
    )

final_df.show()
