# 
# 004 Train model on AirFlow with real=time saving metrics and save model in ML Flow
# 

from airflow import DAG
import logging
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def train_model():
    try:
        import mlflow
        import mlflow.sklearn
        from sklearn.datasets import load_iris
        from sklearn.model_selection import train_test_split
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import accuracy_score
    except ImportError:
        import subprocess
        import sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "scikit-learn", "mlflow"])
        import mlflow
        import mlflow.sklearn
        from sklearn.datasets import load_iris
        from sklearn.model_selection import train_test_split
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import accuracy_score

    # Установите URI для отслеживания MLFlow
    mlflow.set_tracking_uri("http://192.168.0.70:5000/")

    # Начните запуск MLFlow
    with mlflow.start_run():
        # Загрузите данные
        data = load_iris()
        X_train, X_test, y_train, y_test = train_test_split(
            data.data, data.target, test_size=0.2, random_state=42
        )

        # Инициализируйте модель с warm_start=True для добавления деревьев по одному
        model = RandomForestClassifier(
            n_estimators=1,
            warm_start=True,
            random_state=42,
            oob_score=True  # Опционально: использовать out-of-bag оценки
        )

        total_estimators = 10  # Общее количество деревьев
        for i in range(1, total_estimators + 1):
            model.n_estimators = i
            model.fit(X_train, y_train)

            # Сделайте предсказания и вычислите точность
            predictions = model.predict(X_test)
            accuracy = accuracy_score(y_test, predictions)
            print(f"Итерация {i}: Model trained with accuracy: {accuracy}")

            # Логируйте метрику точности в MLFlow
            mlflow.log_metric("accuracy", accuracy, step=i)

            # Опционально: логируйте количество деревьев
            mlflow.log_param("n_estimators", i)

        # Логируйте окончательную модель в MLFlow
        mlflow.sklearn.log_model(model, "random_forest_model")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='train_model_with_mlflow',
    default_args=default_args,
    description='A DAG to train a model on CPU and log metrics with MLFlow',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    train_model_task = PythonOperator(
        task_id='train_model_task',
        python_callable=train_model,
    )

    train_model_task
