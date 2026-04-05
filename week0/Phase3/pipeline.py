from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count

# Create Spark Session
spark = SparkSession.builder.appName("Phase3").getOrCreate()

# ---------------- EXTRACT ----------------
customers = spark.read.option("header", "true").csv("/samples/customers.csv")
orders = spark.read.option("header", "true").csv("/samples/orders.csv")

print("Customers Data:")
customers.show()

print("Orders Data:")
orders.show()

# ---------------- TRANSFORM ----------------

# Clean data
customers = customers.dropna(subset=["customer_id"])
orders = orders.dropna(subset=["customer_id"])

# Join data
df = customers.join(orders, "customer_id")

# Aggregation: Total spend per customer
total_spend = df.groupBy("customer_id", "city") \
    .agg(sum("order_amount").alias("total_spend"))

print("Total Spend per Customer:")
total_spend.show()

# Repeat customers (>1 order)
repeat_customers = orders.groupBy("customer_id") \
    .agg(count("*").alias("order_count")) \
    .filter("order_count > 1")

print("Repeat Customers:")
repeat_customers.show()

# ---------------- LOAD ----------------
print("Final Output:")
total_spend.show()
