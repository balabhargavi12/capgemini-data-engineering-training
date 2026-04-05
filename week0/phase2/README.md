# Phase 2 – Data Transformation using PySpark

## 🔹 Objective

The objective of this phase is to perform data transformation using PySpark by working with multiple datasets. This includes joining tables, applying aggregations, and generating meaningful insights.

---

## 🔹 Tasks Completed

* Loaded customer and order datasets into PySpark DataFrames
* Performed data cleaning by removing null values
* Joined multiple datasets using common keys
* Calculated total order amount per customer
* Identified top 3 customers based on total spend
* Found customers with no orders
* Computed city-wise total revenue
* Calculated average order amount per customer
* Identified customers with more than one order
* Sorted customers based on total spend

---

## 🔹 Approach

1. Created SparkSession and loaded datasets
2. Converted raw data into structured DataFrames
3. Cleaned data by removing null values
4. Applied joins to combine customer and order data
5. Used groupBy and aggregation functions to calculate metrics
6. Applied filtering and sorting operations to derive insights

---

## 🔹 Key Transformations Used

* **join()** → To combine multiple datasets
* **groupBy()** → To group data by specific columns
* **agg()** → To perform calculations like sum, avg, count
* **filter()** → To apply conditions
* **orderBy()** → To sort results

---

## 🔹 Output / Results

* Generated customer-level aggregated data
* Identified top-performing customers
* Calculated revenue insights at city level
* Output screenshots are available in the `outputs/` folder

---

## 🔹 Challenges Faced

* Understanding join conditions between datasets
* Handling missing values in data
* Converting SQL logic into PySpark syntax

---

## 🔹 Learnings

* Learned how to perform joins in PySpark
* Understood the importance of data cleaning
* Gained knowledge of aggregation operations
* Learned how SQL queries map to PySpark transformations

---

## 🔹 Files in this Folder

* `pyspark_code.py` → PySpark implementation
* `sql_queries.sql` → SQL solutions
* `outputs/` → Output screenshots

