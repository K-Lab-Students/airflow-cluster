from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import logging
import time
import wor2vec
from confluent_kafka import Consumer, KafkaException
logging.basicConfig(level=logging.INFO)
consumer = Consumer(consumer_conf)  

consumer_conf = {
    'bootstrap.servers': '192.168.1.50:9092',
    'group.id': 'rutube-consumer-group',
    'auto.offset.reset': 'latest', 
}
def read_data():
    print("Reading data...")
    logging.info("Reading data...")
    consumer.subscribe(['2024_hamidov_daler_voronka_vseros_download_rutube_video'])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            # Message received
            print(f"Received message: {msg.value().decode('utf-8')}")
            logging.info(f"Message received: {msg.value().decode('utf-8')}")
            time.sleep(5)

    except Exception as e:
        logging.error(f"Error while reading messages: {str(e)}")
    finally:
        consumer.close()

def create_embeddings():
    print("Creating embeddings")
    logging.info("Creating embeddings...")
    time.sleep(5)

def building_fass():
    print("Index building")
    logging.info("Index building...")
    time.sleep(5)

def creating():
    print("Creating")
    logging.info("Creating...")
    consumer.subscribe(['2024_hamidov_daler_voronka_vseros_download_rutube_video'])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            # Message received
            print(f"Received message: {msg.value().decode('utf-8')}")
            logging.info(f"Message received: {msg.value().decode('utf-8')}")
            time.sleep(5)

    except Exception as e:
        logging.error(f"Error while reading messages: {str(e)}")
    finally:  
        consumer.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 28),
    'retries': 1,
}

dag = DAG(
    '1024_daler_homidov_main_pypline_prototipe',
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
