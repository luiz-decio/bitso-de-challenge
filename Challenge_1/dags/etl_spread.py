from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

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
    schedule_interval = '*/10 * * * *',
    catchup = False
)

run_etl_spread_btc_mxn = BashOperator(
    task_id = 'run_etl_spread_btc_mxn',
    bash_command = 'python /usr/local/airflow/src/etl_btc_mxn.py',
    dag = dag
)

run_etl_spread_usd_mxn = BashOperator(
    task_id = 'run_etl_spread_usd_mxn',
    bash_command = 'python /usr/local/airflow/src/etl_usd_mxn.py',
    dag = dag
)