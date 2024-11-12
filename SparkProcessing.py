
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# Initialize Spark session with JDBC driver path
spark = SparkSession.builder.appName("Sales Data Processing").config("spark.jars", "C:\\Users\\hmdkr\\spark\\spark-3.5.3-bin-hadoop3\\jars\\postgresql-42.7.4.jar").config("spark.driver.memory", "2g").config("spark.executor.memory", "2g").getOrCreate()

# Load sales data
# Ensure the file path is correctly formatted for Spark on Windows
df = spark.read.csv("file:///C:/Users/hmdkr/Documents/ADataEngineeringUdemyCourse/LargeSaleDataProject/large_sales_data.csv", header=True, inferSchema=True)

# Print the schema (optional)
df.printSchema()

# Calculate total sales per product per day
sales_summary = df.groupBy("Date", "Product").agg(spark_sum(col("Quantity") * col("Price")).alias("Total_Sales"))

# Show the transformed data (optional)
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
        url="jdbc:postgresql://localhost:5432/salesdb",  # Adjust the URL if needed
        table="sales_summary",
        mode="overwrite",  # 'overwrite' to create or replace the table
        properties=db_properties
    )
    print("Data loaded into PostgreSQL")
except Exception as e:
    print("Error during data load:", e)
