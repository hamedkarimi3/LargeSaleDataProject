
#This file is used to run Python scripts 
#for data generation and querying.

FROM python:3.8-slim
WORKDIR /app
COPY . .
RUN pip install pyspark psycopg2
