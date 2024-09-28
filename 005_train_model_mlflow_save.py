# 
# 005 Train Model with real-time ML Flow metric registry and save final model in ML Flow
# 

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta
import os

from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_train_model_command():
    return """
    python -c "
import sys
try:
    from sklearn.datasets import load_iris
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import accuracy_score
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'scikit-learn'])
    from sklearn.datasets import load_iris
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import accuracy_score

data = load_iris()
X_train, X_test, y_train, y_test = train_test_split(
    data.data, data.target, test_size=0.2, random_state=42
)
model = RandomForestClassifier(n_estimators=10, random_state=42)
model.fit(X_train, y_train)
predictions = model.predict(X_test)
accuracy = accuracy_score(y_test, predictions)
print(f'Model trained with accuracy: {accuracy}')
"
    """

with DAG(
    dag_id='train_model_on_cpu',
    default_args=default_args,
    description='A DAG to train a simple model on CPU',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Определение ресурсов для CPU
    cpu_resources = k8s.V1ResourceRequirements(
        requests={
            'memory': '1Gi',
            'cpu': '1'
        },
        limits={
            'memory': '2Gi',
            'cpu': '2'
        }
    )

    train_model_task = KubernetesPodOperator(
        task_id='train_model_task',
        name='train-model-task',
        namespace='default',
        image='python:3.9-slim',  # Используйте подходящий образ с Python
        cmds=["bash", "-c", create_train_model_command()],
        node_selector={'node-type': 'cpu'},  # Убедитесь, что ваши узлы имеют такую метку
        resources=cpu_resources,
        get_logs=True,
        execution_timeout=timedelta(minutes=10),
    )

    train_model_task
