#!/usr/bin/env python
# coding: utf-8

# In[5]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# Initialize Spark session with JDBC driver path (if required)
spark = SparkSession.builder \
    .appName("Sales Data Processing") \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.4.jar") \
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.4.jar") \
    .getOrCreate()

    

# Load sales data
df = spark.read.csv("/opt/bitnami/spark/large_sales_data.csv", header=True, inferSchema=True)

# Print the schema to verify data types (helpful for debugging)
df.printSchema()

# Calculate total sales per product per day
sales_summary = df.groupBy("Date", "Product") \
    .agg(spark_sum(col("Quantity") * col("Price")).alias("Total_Sales"))
# Show the transformed data (optional for verification)
sales_summary.show(10)


# PostgreSQL database connection properties
db_properties = {
    "user": "hk3",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

# Write the DataFrame to PostgreSQL
try:
    sales_summary.write.jdbc(
        url="jdbc:postgresql://postgres:5432/salesdb",  # Corrected to 'postgres' container name
        table="sales_summary",
        mode="overwrite",  # 'overwrite' to create or replace the table
        properties=db_properties
    )
    print("Data loaded into PostgreSQL")
except Exception as e:
    print("Error during data load:", e)

