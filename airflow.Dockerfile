FROM apache/airflow:2.5.1

USER root
RUN apt-get update && apt-get install -y gcc python3-dev && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install pydantic==1.10.13 typing-extensions==4.5.0 clickhouse_driver
