# 🚀 Phase 3A: Data Quality & Cleaning (PySpark)

## 📌 Objective

The objective of this phase is to work with **messy data** and apply data cleaning techniques using PySpark.
This ensures the dataset is accurate and reliable before further processing.

---

## 🔍 Problem Summary

The dataset contained multiple data quality issues such as:

* Missing values (nulls)
* Duplicate records
* Invalid data (negative age)

### 🎯 Tasks Performed

* Identified data issues
* Cleaned the dataset
* Validated cleaned data
* Performed aggregation

---

## ⚙️ Approach

1. Created a DataFrame with messy data
2. Identified null values and duplicates
3. Applied cleaning techniques:

   * Removed rows with null `customer_id`
   * Removed duplicate records
   * Filled missing values (`city`, `name`)
   * Filtered invalid age values (age > 0)
4. Validated results using row counts
5. Performed aggregation (customers per city)

---

## 🔧 Key Transformations Used

* `dropna()` → Remove null values
* `dropDuplicates()` → Remove duplicates
* `fillna()` → Handle missing values
* `filter()` → Remove invalid data
* `groupBy()` → Perform aggregation

---

## 📊 Output / Results

* Cleaned dataset with valid records
* No null or duplicate entries
* Aggregated results showing customer distribution by city

---

## 🧠 Data Engineering Considerations

* Ensured removal of invalid and inconsistent data
* Prevented duplicates from affecting results
* Validated dataset before aggregation

---

## ⚠️ Challenges Faced

* Handling multiple data quality issues simultaneously
* Deciding how to treat missing values without losing useful data

---

## 📚 Learnings

* Real-world data is often messy
* Data cleaning is essential before analysis
* Invalid data leads to incorrect results
* Validation is a critical step in data pipelines

---

## 🏁 Conclusion

This phase demonstrates how PySpark can be used to clean and prepare messy datasets, ensuring high data quality for reliable analysis.

---

