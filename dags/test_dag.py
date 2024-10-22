from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define the default_args dictionary, which includes things like the retry delay, email on failure, etc.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
with DAG(
    'test_dag',  # DAG name
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 22),  # Set the start date
    catchup=False,
    tags=['example'],
) as dag:

    # Define the Python function that will be run by the task
    def print_hello():
        print("Hello, Airflow!")

    # Create the task
    hello_task = PythonOperator(
        task_id='print_hello',  # Task ID
        python_callable=print_hello,  # Function to be called
    )

# Set the task order
hello_task