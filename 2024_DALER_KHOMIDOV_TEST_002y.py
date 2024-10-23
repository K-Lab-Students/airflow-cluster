from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import random
import math

from kubernetes.client import models as k8s  # Import Kubernetes models

default_args = {
    'start_date': days_ago(1),  # Updated start_date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'owner': 'catgirl'
}

def cpu_check():
    # Basic mathematical check for CPU
    result = sum([math.sqrt(i) for i in range(1, 10000)])
    print(f"CPU check result: {result}")

def gpu_check():
    # Simple check using random number generation
    result = sum([random.random() for _ in range(1000000)])
    print(f"GPU check result: {result}")

with DAG(dag_id='multi_node_cluster_test_actual', 
         default_args=default_args, 
         schedule_interval=None, 
         catchup=False,
         tags = ["alive test", "node:cpu", "node:gpu"]) as dag:

    # Define resource requirements
    cpu_intensive_resources = k8s.V1ResourceRequirements(
        requests={
            'memory': '32Gi',
            'cpu': '4'
        },
        limits={
            'memory': '60Gi',
            'cpu': '8'
        }
    )

    gpu_resources = k8s.V1ResourceRequirements(
        limits={
            'nvidia.com/gpu': '1'
        }
    )

    cpu_intensive_task = KubernetesPodOperator(
        task_id='cpu_intensive_task',
        name='cpu-intensive-task',
        namespace='default',
        image='your-cpu-image:latest',
        cmds=["python"],  # Corrected cmds
        arguments=["-c", "import time; time.sleep(10); print('CPU task completed')"],  # Added arguments
        node_selector={'cpu': 'true'},
        resources=cpu_intensive_resources,  # Corrected parameter
        execution_timeout=timedelta(seconds=300),
    )

    gpu_task_1 = KubernetesPodOperator(
        task_id='gpu_task_1',
        name='gpu-task-1',
        namespace='default',
        image='your-gpu-image:latest',
        cmds=["python"],  # Corrected cmds
        arguments=["-c", "import time; time.sleep(5); print('GPU task 1 completed')"],  # Added arguments
        node_selector={'gpu': 'true'},
        resources=gpu_resources,  # Corrected parameter
        execution_timeout=timedelta(seconds=300),
    )

    gpu_task_2 = KubernetesPodOperator(
        task_id='gpu_task_2',
        name='gpu-task-2',
        namespace='default',
        image='your-gpu-image:latest',
        cmds=["python"],  # Corrected cmds
        arguments=["-c", "import time; time.sleep(5); print('GPU task 2 completed')"],  # Added arguments
        node_selector={'gpu': 'true'},
        resources=gpu_resources,  # Corrected parameter
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
