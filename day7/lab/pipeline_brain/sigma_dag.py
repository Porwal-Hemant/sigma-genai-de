from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
import logging
import json

# DAG default arguments
default_args = {
    'owner': 'data-engineering',
   'retries': 2,
   'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
}

# DAG definition
dag = DAG(
    dag_id='sigma_transaction_pipeline',
    default_args=default_args,
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    sla_miss_callback=lambda context: send_email(
        to=["alerts@sigmadatatech.com"],
        subject=f"SLA Miss - DAG: {context['task_instance'].dag_id}, Execution Date: {context['execution_date']}",
        html_content=f"The DAG {context['task_instance'].dag_id} missed its SLA for the execution date {context['execution_date']}."
    ),
    on_failure_callback=lambda context: send_email(
        to=["failures@sigmadatatech.com"],
        subject=f"Failure - DAG: {context['task_instance'].dag_id}, Task: {context['task_instance'].task_id}, Execution Date: {context['execution_date']}",
        html_content=f"The DAG {context['task_instance'].dag_id} failed for task {context['task_instance'].task_id} at execution date {context['execution_date']} with error: {context['exception']}"
    ),
    tags=['sigma', 'transactions', 'daily'],
    description="Daily Bronze->Silver->Gold pipeline for Sigma DataTech transactions"
)

def extract_bronze(**context):
    """Ingest raw CSVs to Bronze Parquet"""
    logging.info(f"Starting extract_bronze task for {context['execution_date']}")
    # CSV ingestion logic here
    logging.info(f"Finished extract_bronze task for {context['execution_date']}")

def transform_silver(**context):
    """Clean, enrich, deduplicate to Silver"""
    logging.info(f"Starting transform_silver task for {context['execution_date']}")
    # Data transformation logic here
    logging.info(f"Finished transform_silver task for {context['execution_date']}")

def build_gold(**context):
    """Generate the 3 Gold aggregation tables"""
    logging.info(f"Starting build_gold task for {context['execution_date']}")
    # Aggregation logic here
    logging.info(f"Finished build_gold task for {context['execution_date']}")

# Task definitions
t1 = PythonOperator(
    task_id='extract_bronze',
    python_callable=extract_bronze,
    on_failure_callback=lambda context: logging.error(f"extract_bronze failed: {context['exception']}"),
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform_silver',
    python_callable=transform_silver,
    on_failure_callback=lambda context: logging.error(f"transform_silver failed: {context['exception']}"),
    dag=dag,
)

t3 = PythonOperator(
    task_id='build_gold',
    python_callable=build_gold,
    on_failure_callback=lambda context: logging.error(f"build_gold failed: {context['exception']}"),
    dag=dag,
)

# Task dependencies
t1 >> t2 >> t3
