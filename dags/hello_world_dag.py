from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define the Python function to be executed
def hello_world():
    print("Hello, World!")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
with DAG('hello_world_dag', default_args=default_args, schedule_interval='@daily') as dag:
    
    # Define the task using PythonOperator
    task_hello_world = PythonOperator(
        task_id='print_hello_world',
        python_callable=hello_world,
    )

# Set the task in the DAG
task_hello_world
