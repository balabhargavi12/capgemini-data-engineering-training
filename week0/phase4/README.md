# 📊 Phase 4 – Business Pipeline & Analytics

## 🔹 1. Objective

To build an end-to-end ETL pipeline using PySpark that processes customer and sales data to generate meaningful business insights.

---

## 🔹 2. Problem Statement

The goal of this project is to analyze customer and sales datasets to extract key insights such as:

* Daily sales trends
* City-wise revenue
* Top 5 customers by spending
* Repeat customers
* Customer segmentation (Gold, Silver, Bronze)
* Final aggregated reporting table

---

## 🔹 3. Approach

### 📥 Extract

* Loaded datasets (`sales.csv`, `customers.csv`) using PySpark
* Verified schema using `show()` and `printSchema()`

### 🔄 Transform

* Cleaned data by removing null and duplicate records
* Filtered invalid values (e.g., negative sales)
* Performed aggregations using `groupBy()` and `agg()`
* Joined datasets using `customer_id`
* Identified top customers using sorting and limiting
* Detected repeat customers using `count()`
* Applied customer segmentation using `when()` conditions
* Created final reporting dataset

### 📤 Load

* Saved final output as CSV file for reporting

---

## 🔹 4. Key Transformations

* `groupBy()` + `agg()` → Aggregations (sum, count)
* `join()` → Data integration
* `filter()` + `dropna()` → Data cleaning
* `orderBy()` + `limit()` → Ranking
* `withColumn()` + `when()` → Business logic
* `concat_ws()` → Full name creation

---

## 🔹 5. Results

* Daily sales summary
* City-wise revenue
* Top 5 customers
* Repeat customers
* Customer segmentation
* Final reporting table

---

## 🔹 6. Final Output Columns

* customer_name
* city
* total_spend
* order_count
* segment

---

## 🔹 7. Data Engineering Considerations

* Ensured data quality by removing null and duplicate records
* Maintained consistent column naming
* Applied correct join logic to avoid data duplication
* Designed output for easy reporting and reuse

---

## 🔹 8. Challenges Faced

* Managing joins without ambiguity
* Handling aggregations correctly
* Implementing segmentation logic
* Structuring final dataset properly

---

## 🔹 9. Learnings

* Built a complete ETL pipeline using PySpark
* Improved understanding of joins and aggregations
* Learned real-world customer segmentation
* Transformed raw data into actionable insights

---

## 🔹 10. Project Structure

* `pyspark_code.py` → PySpark code
* `README.md` → Documentation
* `outputs/` → Result screenshots

---

## 🚀 Conclusion

This project demonstrates how PySpark can be used to build scalable data pipelines and generate business insights efficiently.

