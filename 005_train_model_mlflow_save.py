# 
# 005 Train Model with real-time ML Flow metric registry and save final model in ML Flow
# 

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

# Определение стандартных аргументов
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Определение DAG
with DAG(
    dag_id='train_model_on_cpu_mlflow_save',
    default_args=default_args,
    description='A DAG to train a simple model on CPU',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Определение ресурсов для CPU в виде словаря
    cpu_resources = {
        'request_memory': '1Gi',
        'request_cpu': '1',
        'limit_memory': '2Gi',
        'limit_cpu': '2'
    }

    # Определение Python-кода для выполнения
    train_model_code = """
import sys
import subprocess
import sys
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

# Установка зависимостей, если необходимо
try:
    import sklearn
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "scikit-learn"])

# Функция обучения модели
def train_model():
    data = load_iris()
    X_train, X_test, y_train, y_test = train_test_split(
        data.data, data.target, test_size=0.2, random_state=42
    )
    model = RandomForestClassifier(n_estimators=10, random_state=42)
    model.fit(X_train, y_train)
    predictions = model.predict(X_test)
    accuracy = accuracy_score(y_test, predictions)
    print(f"Model trained with accuracy: {accuracy}")

if __name__ == "__main__":
    train_model()
    """

    # Определение задачи с использованием KubernetesPodOperator
    train_model_task = KubernetesPodOperator(
        task_id='train_model_task',
        name='train-model-task',
        namespace='default',
        image='python:3.9-slim',  # Стандартный Python-образ
        cmds=["python", "-c", train_model_code],
        node_selector={'cpu': 'true'},  # Селектор узлов для CPU
        # resources=cpu_resources,
        get_logs=True,
        execution_timeout=timedelta(minutes=10),
    )

    train_model_task
