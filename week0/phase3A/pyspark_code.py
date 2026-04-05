
# ------------------ IMPORTS ------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ------------------ START SPARK ------------------
spark = SparkSession.builder.appName("Phase3A").getOrCreate()

# ------------------ CREATE DATA ------------------
data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, None, "Chennai", 32),
    (None, "Arun", "Hyderabad", 28),
    (4, "Meena", None, 30),
    (4, "Meena", None, 30),
    (5, "John", "Bangalore", -5)
]

columns = ["customer_id", "name", "city", "age"]

df = spark.createDataFrame(data, columns)

# =================================================
#  1. IDENTIFY ISSUES

print("Original Data:")
df.show()

# Issues:
# - Null values (name, city, customer_id)
# - Duplicate row (Meena)
# - Invalid age (-5)

# =================================================
#  2. CLEAN DATA

# remove null customer_id
df_clean = df.dropna(subset=["customer_id"])

# fill missing values
df_clean = df_clean.fillna({
    "name": "Unknown",
    "city": "Unknown"
})

# remove duplicates
df_clean =df_clean.dropDuplicates()

# remove invalid age
df_clean = df_clean.filter(col("age") > 0)

print("Cleaned Data:")
df_clean.show()

# =================================================
#  3. VALIDATE CLEANING

print("Rows before cleaning:", df.count())
print("Rows after cleaning:", df_clean.count())

# =================================================
# 4. AGGREGATION

print("Customers per City:")
df_clean.groupBy("city").count().show()

# ------------------ STOP ------------------
spark.stop()
