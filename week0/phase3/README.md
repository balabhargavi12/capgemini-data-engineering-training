# 🚀 Phase 3: Final ETL Pipeline (SQL → PySpark)

## 📌 Objective

The objective of this phase is to build a complete **ETL (Extract, Transform, Load) pipeline** using SQL and PySpark.
This phase focuses on transforming raw data into meaningful insights by applying data engineering concepts.

---

## 🔍 Problem Summary

The datasets contained customer and sales information.
The task was to:

* Read and process data
* Clean and validate datasets
* Perform transformations and aggregations
* Build a reusable ETL pipeline

---

## ⚙️ Approach

1. Loaded datasets (`customers.csv`, `sales.csv`) into PySpark DataFrames
2. Inspected schema and data using `show()` and `printSchema()`
3. Performed data cleaning:

   * Removed null values using `dropna()`
   * Filtered invalid records
4. Joined datasets using `customer_id`
5. Applied transformations and aggregations
6. Built a reusable ETL pipeline function

---

## 🔧 Key Transformations Used

* `dropna()` → Handle missing values
* `filter()` → Remove invalid data
* `join()` → Combine datasets
* `groupBy()` → Perform aggregations
* `agg()` → Calculate metrics
* `Window + rank()` → Identify top customers

---

## 📊 Business Problems Solved

### 1. Daily Sales

Calculated total sales for each day.

### 2. City-wise Revenue

Computed total revenue generated per city.

### 3. Repeat Customers

Identified customers with more than 2 orders.

### 4. Highest Spending Customer per City

Used window functions to find top customers in each city.

### 5. Final Reporting Table

Created a summary table with:

* Customer ID
* City
* Total Spend
* Order Count

---

## 📸 Outputs Included

* Customers dataset preview
* Sales dataset preview
* Daily sales
* City-wise revenue
* Repeat customers
* Top customers per city
* Final report

---

## 🧠 Technologies Used

* SQL
* PySpark
* Apache Spark

---

## ⚠️ Challenges Faced

* Handling null values and ensuring data quality
* Converting SQL logic into PySpark transformations
* Implementing window functions correctly

---

## 📚 Learnings

* Understanding complete ETL workflow
* Translating SQL queries into PySpark code
* Working with joins and aggregations
* Using window functions for advanced analysis
* Building reusable data pipelines

---

## 🏁 Conclusion

Phase 3 demonstrates the implementation of a complete ETL pipeline using PySpark, transforming raw data into meaningful insights and simulating real-world data engineering workflows.

---

