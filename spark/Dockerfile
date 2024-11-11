# Use openjdk:11-jre-slim as the base image
FROM openjdk:11-jre-slim

# Install Python, pip, and necessary dependencies
RUN apt-get update && apt-get install -y \
    python3 python3-pip curl && \
    apt-get clean

# Install PySpark
RUN pip3 install pyspark

# Install Spark by downloading it
RUN curl -L https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.2.tgz | tar -xz -C /opt

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark-3.5.3-bin-hadoop3.2
ENV PATH="$SPARK_HOME/bin:$PATH"

# Add PostgreSQL JDBC driver
ADD https://jdbc.postgresql.org/download/postgresql-42.2.5.jar /opt/spark-3.5.3-bin-hadoop3.2/jars/

# Set the working directory (optional based on your project structure)
WORKDIR /app

