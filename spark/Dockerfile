# Use openjdk:11-jre-slim which has Java preinstalled
FROM openjdk:11-jre-slim

# Install Python and pip
RUN apt-get update && apt-get install -y \
    python3 python3-pip curl && \
    apt-get clean

# Install PySpark
RUN pip3 install pyspark

# Install PostgreSQL JDBC driver
ADD https://jdbc.postgresql.org/download/postgresql-42.2.5.jar /opt/spark/jars/

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# Set the working directory
WORKDIR /app
