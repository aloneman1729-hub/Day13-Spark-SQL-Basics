# Day 13 — Spark SQL Basics

## Overview

This project demonstrates the fundamentals of Apache Spark SQL using PySpark DataFrames.

The scenario is a customer analytics system that reads customer data from CSV and JSON files and generates analytical reports using DataFrame operations and Spark SQL.

## Concepts Covered

- Create DataFrame from CSV
- Create DataFrame from JSON
- Inspect schema
- Select columns
- Filter rows
- `withColumn()`
- Spark SQL expressions
- Temporary views
- SQL queries
- Aggregations
- Grouping
- Sorting
- Customer analytics

## Project Structure

```text
Day13-Spark-SQL-Basics/
├── data/
│   ├── customers.csv
│   └── customers.json
├── src/
│   ├── spark_sql_basics.py
│   └── customer_analytics.py
├── tests/
│   └── test_spark_sql.py
├── .gitignore
├── requirements.txt
└── README.md
Data

The customer dataset contains:

Customer ID
Name
City
Age
Segment
Total Spend
Orders

There are 10 customers in the CSV dataset.

DataFrame Operations
Select
df.select("customer_id", "name", "total_spend")
Filter
df.filter(col("segment") == "Premium")
withColumn
df.withColumn(
    "average_order_value",
    col("total_spend") / col("orders")
)
Temporary View

A DataFrame can be registered as a temporary SQL view:

df.createOrReplaceTempView("customers")

After registration, SQL can be executed:

spark.sql("""
    SELECT *
    FROM customers
    WHERE total_spend >= 70000
""")
Analytics

The project generates:

High-value customer report
City-level customer analytics
Customer segment analytics
Overall customer analytics
Running the Project

Activate the virtual environment:

source .venv/bin/activate

Run the main demonstration:

spark-submit src/spark_sql_basics.py

Run customer analytics:

spark-submit src/customer_analytics.py

Run tests:

python -m pytest -v
Expected Test Result
4 passed
Technologies
Python 3.12.3
Apache Spark 3.5.6
PySpark 3.5.6
Spark SQL
pytest 9.1.1
WSL / Ubuntu
Key Learning

Spark SQL provides a structured way to process large datasets using DataFrames and SQL.

The same DataFrame can be manipulated using both DataFrame APIs and SQL queries, making Spark SQL useful for analytics and reporting workloads.
