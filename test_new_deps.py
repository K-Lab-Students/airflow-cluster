from airflow import DAG
import logging
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Функция для тестирования установленной зависимости
def test_dependency():
    try:
        from datasets import load_dataset
        dataset = load_dataset("yelp_review_full")
        store = dataset["train"][100]
        logger.info(f"{store}")
        logger.info("Dependency 'requests' installed successfully and working!")
        print("Dependency 'requests' installed successfully and working!")
    except ImportError as e:
        logger.info(f"Dependency not installed: {e}")
        print(f"Dependency not installed: {e}")

# Определение DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='test_new_deps_dag',
    default_args=default_args,
    description='A DAG to install dependencies and test them',
    schedule_interval=None,  # Выполняется по вызову
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Задача установки зависимостей с помощью pip
    install_dependencies = BashOperator(
        task_id='install_dependencies',
        bash_command='pip install datasets',  # Замените 'requests' на ваши зависимости
    )

    # Задача тестирования установленных зависимостей
    test_dependencies = PythonOperator(
        task_id='test_dependencies',
        python_callable=test_dependency,
    )

    # Определение последовательности выполнения задач
    install_dependencies >> test_dependencies
