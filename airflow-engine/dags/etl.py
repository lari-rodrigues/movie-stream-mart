from datetime import datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

from src import etl_vendor, etl_internal

with DAG(
    "etl_dag",
    start_date=datetime(2023, 2, 1),
    schedule_interval="0 7 1 * *",
    catchup=False
) as dag:
    prefix_internal_path = "data/internal"
    etl_internal = PythonOperator(task_id="etl_internal", python_callable=
    etl_internal.process(movies_path=f"{prefix_internal_path}/movies", streams_path=f"{prefix_internal_path}/streams"))

    prefix_vendor_path = "data/vendor"
    etl_vendor = PythonOperator(task_id="etl_vendor", python_callable=
    etl_vendor.process(authors_path=f"{prefix_vendor_path}/authors", 
            books_path=f"{prefix_vendor_path}/books", 
            reviews_path=f"{prefix_vendor_path}/reviews"))
    

    