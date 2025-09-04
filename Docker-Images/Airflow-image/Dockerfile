FROM apache/airflow:2.8.0
USER root
COPY Airflow-image/requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt