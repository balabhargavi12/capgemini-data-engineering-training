# 📊 Day 4 – String Functions & Student Submission Analysis

## 🚀 Overview  
Day 4 focuses on two key areas:
1. **Data Cleaning using SQL String Functions**
2. **Real-world Student Submission Analysis using SQL & PySpark**

This phase emphasizes transforming messy data into a clean, structured format and performing analysis using joins and window functions.

---

## 🎯 Objectives  

- Clean and standardize text data using **String Functions**  
- Handle inconsistent formats like spaces, casing, and naming issues  
- Analyze student submission data from multiple datasets  
- Detect duplicates and classify submission status  
- Build a structured data processing pipeline  

---

## 🔹 Part 1: String Functions (Data Cleaning)

### 🔸 Problem  
Datasets contained:
- Inconsistent text formats (uppercase/lowercase)  
- Extra spaces in values  
- Improperly formatted names and emails  

### 🔸 Transformations Performed  

- Converted text to lowercase/uppercase (`LOWER`, `UPPER`)  
- Trimmed spaces (`TRIM`, `LTRIM`, `RTRIM`)  
- Formatted strings into proper structure  
- Extracted substrings where required  

📌 **Outcome:**  
- Clean, consistent, and readable text data  

---

## 🔹 Part 2: Student Submission Analysis

### 🔸 Problem Summary  

Worked with multiple datasets:
- Student Master Data  
- Task Responses Dataset 1  
- Task Responses Dataset 2 (duplicates + invalid records)  

### 🔸 Goals  

- Clean and normalize datasets  
- Map multiple emails to a single student  
- Identify valid, invalid, and missing submissions  
- Detect duplicate submissions  
- Classify all students based on submission status  

---

## 🔹 Approach  

### **1. Data Cleaning**
- Removed unwanted spaces in column names  
- Standardized email formats (lowercase + trimmed)  

### **2. Data Merging**
- Combined datasets using **Full Outer Join**  
- Used `COALESCE` to handle missing values  
- Removed duplicate email conflicts  

### **3. Core Analysis**
- **Valid Submissions** → Inner Join with student data  
- **Invalid Submissions** → Left Anti Join  
- **Not Submitted** → No matching records  

### **4. Duplicate Detection**
Used window function:

```sql
ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY submission_time)
