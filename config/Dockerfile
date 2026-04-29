FROM apache/airflow:2.7.1 
USER root
RUN apt-get update && apt-get install -y build-essential
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt