from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define a Python function for testing
def test_function():
    print("Test function executed")

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 28),
    'retries': 1,
}

# Initialize the DAG
dag = DAG(
    'test_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval='@daily',  # Runs daily
)

# Define tasks
start_task = DummyOperator(
    task_id='start_task',
    dag=dag,
)

test_python_task = PythonOperator(
    task_id='test_python_task',
    python_callable=test_function,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

# Define task dependencies
start_task >> test_python_task >> end_task
