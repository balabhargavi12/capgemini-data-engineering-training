# 📊 Insurance Data Analysis using PySpark & SQL

## 📌 Project Overview
This project focuses on analyzing insurance data using **PySpark** and **SQL** to identify customer risk, validate data quality, and generate meaningful business insights.

The dataset includes:
- Customers
- Policies
- Claims
- Agents
- Policy-Agent Mapping

---

## 🎯 Objectives
- Perform data understanding and cleaning
- Build relationships between datasets
- Compute key business metrics
- Identify high-risk customers
- Apply advanced SQL concepts:
  - Subqueries
  - CTEs (Common Table Expressions)
  - Window Functions

---

## 🧩 Project Workflow

### 🔹 Phase 1 – Data Understanding
- Explored dataset structure and relationships
- Performed schema validation
- Checked for:
  - Null values
  - Invalid/negative values
- Established relationship flow:

  
---

### 🔹 Phase 2 – Data Cleaning
- Handled missing values
- Removed invalid data (e.g., negative premiums/claims)
- Standardized text columns
- Corrected data types
- Ensured referential integrity

---

### 🔹 Phase 3 – Data Transformation
Derived key business metrics:
- Total premium per customer
- Total claims per customer
- Risk score (claim-to-premium ratio)

---

### 🔹 Phase 4 – SQL Analysis (Subqueries)
- Solved business problems using SQL
- Identified:
- Top customers
- Repeat customers
- Trends over time

#### ✅ Subqueries Used:
- Scalar subqueries
- Filtering subqueries
- Aggregation-based subqueries

---

### 🔹 Phase 5 – Advanced SQL (CTEs)
- Simplified complex queries using CTEs

#### ✅ CTE Use Cases:
- Intermediate calculations
- Reusable query logic
- Simplifying joins and aggregations

---

### 🔹 Phase 6 – Window Functions
Used for:
- Ranking customers and agents
- Comparing values across rows
- Time-based analysis

---

### 🔹 Phase 7 – Final Output
- Combined processed data into a final dataset
- Added derived columns (risk indicators)
- Stored output in efficient formats (Parquet)

---

## ✅ Data Validation
- Verified record counts before & after transformations
- Ensed no duplicate records
- Validated null handling
- Maintained referential integrity

---

## 📈 Key Insights
- Identified high-risk customers
- Detected patterns in claims and premiums
- Evaluated agent performance
- Analyzed customer distribution across cities

---

## 🛠️ Technologies Used
- **PySpark** → Data processing & transformation  
- **SQL** → Data analysis  
- **Parquet** → Efficient data storage  

---

## 🚀 Conclusion
This project demonstrates a complete **data engineering + analytics pipeline**, including:
- Data understanding
- Data cleaning
- Transformation
- Advanced SQL analysis

The use of **Subqueries, CTEs, and Window Functions** enabled solving complex business problems in a structured and efficient way.

---

## 📬 Contact
If you like this project or have suggestions, feel free to connect!
