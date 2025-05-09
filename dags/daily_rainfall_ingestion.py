from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
import include.ingest_rainfall as ir
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import logging

# Create logger and define threshold
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Get the current date - a day before the airflow's schedule date
scheduled_date = datetime.now(ZoneInfo('Asia/Singapore'))
current_date = scheduled_date - timedelta(days=1)
current_date_str = current_date.strftime('%Y-%m-%d')

# Set month and year for email subject
month_year_str_email_subject = current_date.strftime('%B, %Y') 

# Set default arguments for task
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
}

# Define DAG
with DAG(
    dag_id='daily_rainfall_ingestion',
    default_args=default_args,
    start_date=datetime(2025, 5, 1),
    schedule='15 0 * * *',
    catchup=False,
) as dag:
    
    # Task to ingest raw rainfall data
    ingest_task = PythonOperator(
        task_id='ingest_raw_data',
        python_callable=ir.ingest_rainfall_data,
        op_kwargs={
            'start_date_str': current_date_str,
            'end_date_str': current_date_str,
        },
    )

    # Task to update hourly rainfall data
    update_hourly_task = PythonOperator(
        task_id='update_hourly_rainfall',
        python_callable=ir.load_hourly_reading_data,
        op_kwargs={
            'start_date_str': current_date_str,
            'end_date_str': current_date_str,
        },
    )

    # Task to check for alert
    check_alert_task = PythonOperator(
        task_id='check_for_alert',
        python_callable=ir.check_for_alert,
        op_kwargs={
            'current_date_str': current_date_str,
        },
    )

    # Task to send alert via email
    send_alert_task = EmailOperator(
        task_id='send_alert',
        to='quahho@gmail.com',
        subject=f'🚨 Rainfall Alert: Station(s) Exceeding Monthly Average in {month_year_str_email_subject}',
        html_content="{{ ti.xcom_pull(key='email_content', task_ids='check_for_alert') | replace('\n', '<br>') }}",
    )

    # Task to update hourly rainfall data
    refresh_hist_task = PythonOperator(
        task_id='refresh_hist_monthly_avg',
        python_callable=ir.refresh_hist_monthly_avg,
        op_kwargs={
            'current_date_str': current_date_str,
        },
    )

    # Set dependencies
    ingest_task >> update_hourly_task >> check_alert_task >> send_alert_task >> refresh_hist_task
