from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Функция для тестирования установленной зависимости
def test_dependency():
    try:
        from datasets import load_dataset
        dataset = load_dataset("yelp_review_full")
        dataset["train"][100]
        print("Dependency 'requests' installed successfully and working!")
    except ImportError as e:
        print(f"Dependency not installed: {e}")

# Определение DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='install_and_test_dependencies',
    default_args=default_args,
    description='A DAG to install dependencies and test them',
    schedule_interval=None,  # Выполняется по вызову
    start_date=datetime(2023, 1, 1),
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
