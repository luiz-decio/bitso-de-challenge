from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from utils import calculate_spread

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

# Define the DAG
dag = DAG(
    'get_orders_spread',
    default_args = default_args,
    description = 'Get the spread from orders each 10 minutes.',
    schedule_interval = timedelta(minutes = 10),
    catchup = False
)

def run_pipeline():
    observations = []

    for _ in range(600):
        pass