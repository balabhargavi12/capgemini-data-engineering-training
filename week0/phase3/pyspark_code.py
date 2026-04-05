# imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, desc, rank
from pyspark.sql.window import Window

# start spark
spark = SparkSession.builder.appName("ETL_Pipeline").getOrCreate()

# load data
customers = spark.read.csv("/samples/customers.csv", header=True, inferSchema=True)
sales = spark.read.csv("/samples/sales.csv", header=True, inferSchema=True)

# clean data
customers = customers.dropna()
sales = sales.dropna()

# filter invalid records
sales = sales.filter((col("quantity") > 0) & (col("total_amount") > 0))

# join
data = sales.join(customers, "customer_id")

# daily sales
daily_sales = sales.groupBy("sale_date").agg(sum("total_amount").alias("daily_sales"))

# city revenue
city_revenue = data.groupBy("city").agg(sum("total_amount").alias("city_revenue"))

# repeat customers
repeat_customers = sales.groupBy("customer_id").agg(count("*").alias("order_count")).filter(col("order_count") >= 2)

# top customer per city
spend = data.groupBy("customer_id", "city").agg(sum("total_amount").alias("total_spend"))

window = Window.partitionBy("city").orderBy(desc("total_spend"))

top_customers = spend.withColumn("rank", rank().over(window)).filter(col("rank") == 1)

# final report
final_report = data.groupBy("customer_id", "city").agg(sum("total_amount").alias("total_spend"),count("*").alias("order_count"))

# output
daily_sales.show()
city_revenue.show()
repeat_customers.show()
top_customers.show()
final_report.show()
