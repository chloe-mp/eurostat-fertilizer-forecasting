FROM apache/airflow:2.10.3
USER root
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
