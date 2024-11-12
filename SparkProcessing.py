from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# Initialize Spark session with JDBC driver path
spark = SparkSession.builder \
    .appName("Sales Data Processing") \
        .config("spark.jars", "path/to/postgresql-42.7.4.jar") \
        .getOrCreate()

# Load sales data
df = spark.read.csv("path/to/large_sales_data.csv", header=True, inferSchema=True)  # Update the path here

# Print the schema (optional)
df.printSchema()

# Calculate total sales per product per day
sales_summary = df.groupBy("Date", "Product") \
    .agg(spark_sum(col("Quantity") * col("Price")).alias("Total_Sales"))

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
        url="jdbc:postgresql://postgres:5432/salesdb",
        table="sales_summary",
        mode="overwrite",  # 'overwrite' to create or replace the table
        properties=db_properties
    )
    print("Data loaded into PostgreSQL")
except Exception as e:
    print("Error during data load:", e)
