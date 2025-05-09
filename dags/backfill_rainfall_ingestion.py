# Import libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
import include.ingest_rainfall as ir
from datetime import datetime, timedelta
import logging

# Create logger and define threshold
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Set start and end date
start_date_str = '2023-04-01'
end_date_str = '2023-05-31'

# Set default arguments for task
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id='backfill_rainfall_ingestion',
    default_args=default_args,
    start_date=datetime(2025, 5, 1),
    schedule=None,
    catchup=False,
) as dag:
    
    # Task to ingest raw rainfall data
    ingest_task = PythonOperator(
        task_id='ingest_raw_data',
        python_callable=ir.ingest_rainfall_data,
        op_kwargs={
            'start_date_str': start_date_str,
            'end_date_str': end_date_str,
        },
    )

    # Task to update hourly rainfall data
    update_hourly_task = PythonOperator(
        task_id='update_hourly_rainfall',
        python_callable=ir.load_hourly_reading_data,
        op_kwargs={
            'start_date_str': start_date_str,
            'end_date_str': end_date_str,
        },
    )

    # Set dependencies
    ingest_task >> update_hourly_task 
