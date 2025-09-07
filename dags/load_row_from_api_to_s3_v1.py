from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import requests
from datetime import timedelta, datetime
import boto3
import json

def get_query_for_day(**context):
    exec_date = context["execution_date"]
    start_date = (exec_date - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = exec_date.strftime("%Y-%m-%d")

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "starttime": start_date,
        "endtime": end_date,
    }

    response = requests.get(url=url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json() 

    json_bytes = json.dumps(data).encode("utf-8")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        region_name="us-east-1",
        endpoint_url="http://minio:9000"
    )

    bucket = "raw-layer"
    key = f"earthquake/{(exec_date - timedelta(days=1)).strftime('%Y/%m/%d')}.json"

    s3.put_object(Bucket=bucket, Key=key, Body=json_bytes)  

default_args = {
    "owner": "admin",
}

with DAG(
    dag_id="load_row_from_api_to_s3",
    default_args=default_args,
    start_date=datetime(2025, 9, 6),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task_1 = PythonOperator(
        task_id="from_api_to_s3",
        python_callable=get_query_for_day
    )
    task_1
