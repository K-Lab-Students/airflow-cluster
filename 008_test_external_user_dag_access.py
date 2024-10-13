from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello, World!")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

# Set the category of the DAG
category = 'External User'

# Define access control based on category
access_control = {}
if category == 'External User':
    access_control = {
        'external_user': {'can_read', 'can_edit'}
    }

# Define the DAG with access control
with DAG(
    dag_id='hello_world_external_user1',
    default_args=default_args,
    schedule_interval='@daily',
    access_control=access_control,
    catchup=False,
) as dag:

    # Define the task
    hello_task = PythonOperator(
        task_id='hello_world_task',
        python_callable=hello_world,
    )
