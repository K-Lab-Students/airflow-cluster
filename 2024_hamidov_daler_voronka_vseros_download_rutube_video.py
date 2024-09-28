from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import logger
import time
import wor2vec

def read_data():
    print("Reading data")
    logger.info("Reading data...")
    time.sleep(5)

def create_embeddings():
    print("Creating embeddings")
    logger.info("Creating embeddings...")
    time.sleep(5)

def building_fass():
    print("Index building")
    logger.info("Index building...")
    time.sleep(5)

def creating():
    print("Creating")
    logger.info("Creating...")
    time.sleep(5)   


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 28),
    'retries': 1,
}

dag = DAG(
    '2024_hamidov_daler_voronka_vseros_download_rutube_video',
    default_args=default_args,
    description='A basic test DAG',
    schedule_interval='@daily', 
)

start_task = DummyOperator(
    task_id='start_task',
    dag=dag,
)

loadind_data = PythonOperator(
    task_id='test_python_task',
    python_callable=read_data,
    dag=dag,
)

creating_embedd = PythonOperator(
    task_id='test_python_task',
    python_callable=create_embeddings,
    dag=dag,
)

building_fass = PythonOperator(
    task_id='test_python_task',
    python_callable=building_fass,
    dag=dag,
)

generating_ans = PythonOperator(
    task_id='test_python_task',
    python_callable=creating,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

start_task >> loadind_data >>  creating_embedd >> building_fass >> generating_ans >> end_task
