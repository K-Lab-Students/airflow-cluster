from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import math

default_args = {
    'start_date': datetime(2024, 11, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def cpu_check():
    # Базовая математическая проверка для CPU
    result = sum([math.sqrt(i) for i in range(1, 10000)])
    print(f"CPU check result: {result}")

def gpu_check():
    # Простая проверка с использованием генерации случайных чисел
    result = sum([random.random() for _ in range(1000000)])
    print(f"GPU check result: {result}")

with DAG('test_cluster_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    cpu_intensive_task = KubernetesPodOperator(
        task_id='cpu_intensive_task',
        name='cpu-intensive-task',
        namespace='default',
        image='your-cpu-image:latest',
        cmds=["python", "-c", "import time; time.sleep(10); print('CPU task completed')"],
        node_selector={'cpu': 'true'},
        resources={
            'requests': {
                'memory': '32Gi',
                'cpu': '4'
            },
            'limits': {
                'memory': '60Gi',
                'cpu': '8'
            }
        },
        execution_timeout=timedelta(seconds=300),
    )

    gpu_task_1 = KubernetesPodOperator(
        task_id='gpu_task_1',
        name='gpu-task-1',
        namespace='default',
        image='your-gpu-image:latest',
        cmds=["python", "-c", "import time; time.sleep(5); print('GPU task 1 completed')"],
        node_selector={'gpu': 'true'},
        resources={
            'limits': {
                'nvidia.com/gpu': '1'
            }
        },
        execution_timeout=timedelta(seconds=300),
    )

    gpu_task_2 = KubernetesPodOperator(
        task_id='gpu_task_2',
        name='gpu-task-2',
        namespace='default',
        image='your-gpu-image:latest',
        cmds=["python", "-c", "import time; time.sleep(5); print('GPU task 2 completed')"],
        node_selector={'gpu': 'true'},
        resources={
            'limits': {
                'nvidia.com/gpu': '1'
            }
        },
        execution_timeout=timedelta(seconds=300),
    )

    cpu_check_task = PythonOperator(
        task_id='cpu_check_task',
        python_callable=cpu_check,
    )

    gpu_check_task = PythonOperator(
        task_id='gpu_check_task',
        python_callable=gpu_check,
    )

    cpu_intensive_task >> [gpu_task_1, gpu_task_2] >> cpu_check_task >> gpu_check_task
