# 
# 002 Install Deps for Python [task 1] and Test download Dataset [task 2] and store to AirFlow Datasets [task 3]
# 

from airflow import DAG
import logging
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime, timedelta
import os

logger = logging.getLogger(__name__)

# Определение URI для сохранения датасета
dataset_uri = "file:///tmp/airflow_datasets/yelp_review_record.txt"  # Можно настроить путь по необходимости
example_dataset = Dataset(dataset_uri)

# Функция для тестирования установленной зависимости
def test_dependency(**kwargs):
    try:
        from datasets import load_dataset
        dataset = load_dataset("yelp_review_full")
        store = dataset["train"][100]
        logger.info(f"{store}")
        logger.info("Dependency 'datasets' installed successfully and working!")
        print("Dependency 'datasets' installed successfully and working!")

        # Push the dataset record to XCom for downstream tasks
        kwargs['ti'].xcom_push(key='dataset_record', value=store)
        
    except ImportError as e:
        logger.error(f"Dependency not installed: {e}")
        print(f"Dependency not installed: {e}")
        raise  # Fail the task if dependency is not installed

# Функция для сохранения данных набора данных
def save_dataset(**kwargs):
    # Pull the dataset record from XCom
    ti = kwargs['ti']
    store = ti.xcom_pull(key='dataset_record', task_ids='test_dependencies')
    
    if store:
        # Определите путь для сохранения файла
        save_path = '/tmp/dataset_record.txt'  # Вы можете изменить путь по необходимости
        
        try:
            # Сохранение данных в файл
            with open(save_path, 'w') as f:
                f.write(str(store))
            logger.info(f"Dataset record saved successfully at {save_path}.")
            print(f"Dataset record saved successfully at {save_path}.")
        except Exception as e:
            logger.error(f"Failed to save dataset record: {e}")
            print(f"Failed to save dataset record: {e}")
            raise  # Fail the task if saving fails
    else:
        logger.error("No dataset record found to save.")
        print("No dataset record found to save.")
        raise ValueError("No dataset record found to save.")

# Определение DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='download_hugging_face_dataset_and_save',
    default_args=default_args,
    description='A DAG to install dataset dependencies [task 1] and download the dataset from HuggingFace [task 2] and store to AirFlow Datasets [task 3]',
    schedule_interval=None,  # Выполняется по вызову
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Задача установки зависимостей с помощью pip
    install_dependencies = BashOperator(
        task_id='install_dependencies',
        bash_command='pip install datasets',  # Замените 'datasets' на ваши зависимости, если необходимо
    )

    # Задача тестирования установленных зависимостей
    test_dependencies = PythonOperator(
        task_id='test_dependencies',
        python_callable=test_dependency,
        provide_context=True,  # Позволяет передавать контекст в функцию
    )

    # Задача сохранения записи набора данных
    save_dataset_task = PythonOperator(
        task_id='save_dataset',
        python_callable=save_dataset,
        provide_context=True,  # Позволяет передавать контекст в функцию
        outlets=[example_dataset],  # Указываем датасет для записи в Dataset вкладку
    )

    # Определение последовательности выполнения задач
    install_dependencies >> test_dependencies >> save_dataset_task
