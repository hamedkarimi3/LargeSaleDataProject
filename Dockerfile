# Use python:3.8 as the base image
FROM python:3.8

# Install OpenJDK 11 and other dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    curl gnupg && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Python dependencies
RUN pip install pyspark psycopg2-binary

# Set the working directory
WORKDIR /app

# Copy the project files into the container
COPY . .

# Set the command to run the Spark job
ENTRYPOINT ["python", "/app/SparkProcessing.py"]
