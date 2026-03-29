from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Phase1").getOrCreate()

# Create DataFrame
customers = spark.createDataFrame([
    (1, "Ravi", "Hyderabad", 25),
    (2, "Sita", "Chennai", 32),
    (3, "Arun", "Hyderabad", 28)
], ["customer_id", "customer_name", "city", "age"])

# 1. Show all customers
customers.show()

# 2. Filter Chennai
customers.filter(customers.city == "Chennai").show()

# 3. Age > 25
customers.filter(customers.age > 25).show()

# 4. Select columns
customers.select("customer_name", "city").show()

# 5. GroupBy count
customers.groupBy("city").count().show()
