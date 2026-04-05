# 📊 Phase 4A – Bucketing & Segmentation in PySpark

## 🔹 Objective

To understand how continuous data can be converted into categories (bucketing/segmentation) and implement different segmentation techniques using PySpark.

---

## 🔹 What is Bucketing?

Bucketing (or segmentation) is the process of dividing continuous numerical values into categories such as:

* Gold
* Silver
* Bronze

This helps simplify analysis and supports better business decision-making.

---

## 🔹 Dataset Used

* sales.csv
* customers.csv

---

## 🔹 Steps Performed

### 1️⃣ Data Loading

* Loaded datasets using PySpark DataFrames
* Verified schema and data

### 2️⃣ Data Cleaning

* Removed null values in `customer_id`
* Removed duplicate records
* Filtered invalid values (`total_amount > 0`)

### 3️⃣ Data Transformation

* Joined customer and sales data using `customer_id`
* Calculated total spend per customer

---

## 🔹 Segmentation Methods Implemented

### ✅ 1. Conditional Logic

* Used `when()` to create segments based on fixed thresholds:

  * Gold → total_spend > 10000
  * Silver → 5000–10000
  * Bronze → < 5000

---

### ✅ 2. Quantile-based Segmentation

* Used `approxQuantile()` to dynamically divide customers:

  * Bottom → Bronze
  * Middle → Silver
  * Top → Gold

---

### ✅ 3. Bucketizer (MLlib)

* Used predefined splits to assign bucket values
* Converted numerical data into categorical buckets

---

### ✅ 4. Window Function (Ranking)

* Used `percent_rank()` to rank customers based on spending
* Helps understand relative performance

---

## 🔹 Key Transformations

* `groupBy()` + `agg()` → Aggregation
* `join()` → Data integration
* `filter()` + `dropna()` → Data cleaning
* `when()` → Conditional segmentation
* `approxQuantile()` → Dynamic segmentation
* `Bucketizer` → Fixed bucket creation
* `percent_rank()` → Ranking

---

## 🔹 Results

* Created customer segments using multiple methods
* Compared fixed and dynamic segmentation techniques
* Generated insights into customer distribution

---

## 🔹 Reflection Answers

**1. Why convert continuous values into categories?**
To simplify analysis and make data easier to interpret for business decisions.

**2. Difference between business segmentation and technical bucketing?**
Business segmentation uses meaningful labels (Gold, Silver, Bronze), while technical bucketing groups values into numeric ranges.

**3. When would fixed thresholds fail?**
When data distribution changes or varies over time.

**4. How does quantile segmentation differ from fixed rules?**
Quantile segmentation adapts to data distribution, while fixed rules remain constant.

**5. Which method is best for real-world use?**
Quantile-based segmentation is more flexible and suitable for dynamic datasets.

---

## 🔹 Project Structure

* `phase4a.py` → PySpark implementation
* `README.md` → Documentation
* `outputs/` → Screenshots

---

## 🚀 Conclusion

This phase demonstrates how different segmentation techniques can be applied to transform raw numerical data into meaningful categories, enabling better analysis and decision-making.

