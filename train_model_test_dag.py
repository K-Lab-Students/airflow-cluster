from airflow import DAG
import logging
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score


def train_model():
    data = load_iris()
    X_train, X_test, y_train, y_test = train_test_split(data.data, data.target, test_size=0.2, random_state=42)
    model = RandomForestClassifier(n_estimators=10, random_state=42)
    model.fit(X_train, y_train)
    predictions = model.predict(X_test)
    accuracy = accuracy_score(y_test, predictions)
    print(f"Model trained with accuracy: {accuracy}")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='train_model_on_cpu',
    default_args=default_args,
    description='A DAG to train a simple model on CPU',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Задача для установки scikit-learn
    install_dependencies = BashOperator(
        task_id='install_dependencies',
        bash_command='pip install scikit-learn',  # Команда для установки scikit-learn
    )

    train_model_task = PythonOperator(
        task_id='train_model_task',
        python_callable=train_model,
    )

    install_dependencies >> train_model_task
