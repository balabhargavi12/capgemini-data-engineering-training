from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count

# Create Spark session
spark = SparkSession.builder.appName("Phase2").getOrCreate()

# Create Customers Data
customers_data = [
    ("1", "Amit", "Hyderabad"),
    ("2", "Sneha", "Bangalore"),
    ("3", "Rahul", "Chennai"),
    ("4", "Priya", "Hyderabad")
]

customers = spark.createDataFrame(customers_data, ["customer_id", "name", "city"])

# Create Orders Data
orders_data = [
    ("101", "1", 500),
    ("102", "1", 300),
    ("103", "2", 700),
    ("104", "3", 200),
    ("105", "3", 100)
]

orders = spark.createDataFrame(orders_data, ["order_id", "customer_id", "order_amount"])

# Show data
customers.show()
orders.show()

customers = customers.dropna(subset=["customer_id"])
orders = orders.dropna(subset=["customer_id"])

# 1. Total order amount for each customer
orders.groupBy("customer_id").sum("order_amount").show()

# 2. Top 3 customers by total spend
orders.groupBy("customer_id") \
      .agg(sum("order_amount").alias("total")) \
      .orderBy("total", ascending=False) \
      .show(3)

# 3. Customers with no orders
customers.join(orders, "customer_id", "left_anti").show()

# 4. City-wise total revenue
customers.join(orders, "customer_id") \
         .groupBy("city") \
         .sum("order_amount") \
         .show()

# 5. Average order amount per customer
orders.groupBy("customer_id") \
      .agg(avg("order_amount")) \
      .show()

# 6. Customers with more than one order
orders.groupBy("customer_id") \
      .agg(count("*").alias("cnt")) \
      .filter("cnt > 1") \
      .show()

# 7. Sort customers by total spend descending
orders.groupBy("customer_id") \
      .sum("order_amount") \
      .orderBy("sum(order_amount)", ascending=False) \
      .show()
