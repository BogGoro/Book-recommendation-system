FROM apache/airflow:2.5.1-python3.9

USER airflow
RUN pip install --no-cache-dir pydantic==1.10.13 typing-extensions==4.5.0 clickhouse_driver
