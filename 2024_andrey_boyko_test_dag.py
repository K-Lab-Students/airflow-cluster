from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import pendulum

moscow_tz = pendulum.timezone("Europe/Moscow")

def print_message(**kwargs):
	print("На часах 00")

# Определяем DAG
default_args = {
	'owner': 'airflow',
	'start_date': pendulum.datetime(2024, 1, 1, tz=moscow_tz),
	'retries': 0,
	'retry_delay': timedelta(minutes=5),
}

with DAG(
	dag_id='2024_andrey_boyko_test_dag',
	default_args=default_args,
	description='Wholesome test DAG',
	schedule_interval='0 0 * * *',
	catchup=False,
	tags = ["example"]
) as dag:
   task_print_message = PythonOperator(
		task_id='print_message_at_00',
		python_callable=print_message,
	)