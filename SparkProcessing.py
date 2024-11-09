from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# Initialize Spark session with JDBC driver path (if required)
spark = SparkSession.builder \
    .appName("Sales Data Processing") \
    .config("spark.jars", "C:/Users/hmdkr/spark/spark-3.5.3-bin-hadoop3/jars/postgresql-42.7.4.jar") \
    .getOrCreate()

# Load sales data
df = spark.read.csv("C:/Users/hmdkr/Documents/ADataEngineeringUdemyCourse/LargeSaleDataProject/large_sales_data.csv", header=True, inferSchema=True)

# Calculate total sales per product per day
sales_summary = df.groupBy("Date", "Product") \
    .agg(spark_sum(col("Quantity") * col("Price")).alias("Total_Sales"))

# PostgreSQL database connection properties
db_properties = {
    "user": "hk3",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

# Write the DataFrame to PostgreSQL
sales_summary.write.jdbc(
    url="jdbc:postgresql://localhost:5432/salesdb",
    table="sales_summary",
    mode="overwrite",
    properties=db_properties
)

print("Data loaded into PostgreSQL")
