from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(
    dag_id='etl_dag',
    default_args=default_args,
    description='ETL DAG for extraction and transformation of finance data',
    schedule_interval='0 */12 * * *',  # Run every 12 hours
    start_date=datetime(2024, 8, 3),  # Set this to your desired start date
    catchup=False,
) as dag:

    # Define the tasks
    extract_task = BashOperator(
        task_id='run_extraction',
        bash_command='python usr/local/airflow/src/extraction.py',
    )

    transform_load_task = BashOperator(
        task_id='run_transform_load',
        bash_command='python usr/local/airflow/src/transform_load.py',
    )

    # Set task dependencies
    extract_task >> transform_load_task