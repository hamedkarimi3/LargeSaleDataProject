# Use a base image with Python and OpenJDK
FROM openjdk:11-jre-slim

# Install Python and dependencies
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean

# Install PySpark and PostgreSQL JDBC driver
RUN pip3 install pyspark
ADD https://jdbc.postgresql.org/download/postgresql-42.2.5.jar /opt/spark/jars/

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

