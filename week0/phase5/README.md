# Phase 5 – Databricks + Olist End-to-End Data Engineering Pipeline

## 📌 Objective
The objective of this phase is to build a complete data engineering pipeline using a real-world dataset. This includes data ingestion, transformation, analysis, and reporting using PySpark and SQL in Databricks.

---

## 📂 Dataset
We used the **Olist Brazilian E-commerce Dataset** from Kaggle.

- Contains multiple tables like customers, orders, payments, products, and order items
- Represents real-world e-commerce transactions

---

## 🛠️ Tools & Technologies
- Databricks Community Edition
- PySpark
- SQL
- GitHub

---

## ⚙️ Steps Performed

### 1. Data Ingestion
- Downloaded dataset from Kaggle
- Uploaded all CSV files into Databricks (`/FileStore/olist/`)
- Loaded data using PySpark DataFrames

### 2. Data Processing
- Performed joins between multiple tables
- Applied aggregations and transformations
- Ensured data consistency and correctness

### 3. Analytical Tasks

#### ✅ Task 1: Top 3 Customers per City
- Calculated total spend per customer
- Used `ROW_NUMBER()` window function to rank customers within each city

#### ✅ Task 2: Running Total of Sales
- Computed daily sales
- Used window function to calculate cumulative sales

#### ✅ Task 3: Top Products per Category
- Aggregated product sales
- Applied `DENSE_RANK()` to find top products in each category

#### ✅ Task 4: Customer Lifetime Value
- Calculated total spending of each customer

#### ✅ Task 5: Customer Segmentation
- Segmented customers into:
  - Gold (>10000)
  - Silver (5000–10000)
  - Bronze (<5000)

#### ✅ Task 6: Final Reporting Table
- Created a consolidated dataset with:
  - customer_id
  - customer_city
  - total_spend
  - segment
  - total_orders

---

## 📊 Outputs
All outputs (screenshots/results) are available in the `outputs/` folder.

---

## 🧠 Key Learnings
- Working with real-world datasets
- Understanding fact and dimension tables
- Using window functions in PySpark & SQL
- Building end-to-end data pipelines
- Handling joins and aggregations efficiently

---

## 🚀 Conclusion
This phase demonstrates the implementation of a complete data engineering workflow using Databricks, showcasing both PySpark and SQL capabilities on a real-world dataset.

