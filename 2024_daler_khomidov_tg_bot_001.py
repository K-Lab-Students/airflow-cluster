from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import random
import math
from kubernetes import client, config
import requests
import os

# Default arguments for the DAG
default_args = {
    'owner': 'кошкодевочка',  # Owner of the DAG
    'start_date': datetime(2024, 11, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    'multi_node_cluster_test',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["example", "node:cpu", "node:gpu"],
) as dag:

    # Define resource requirements
    from kubernetes.client import models as k8s

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

    # Task: CPU Intensive Task (KubernetesPodOperator)
    cpu_intensive_task = KubernetesPodOperator(
        task_id='cpu_intensive_task',
        name='cpu-intensive-task',
        namespace='default',
        image='your-cpu-image:latest',
        cmds=["python", "-c", "import time; time.sleep(10); print('CPU task completed')"],
        node_selector={'cpu': 'true'},
        container_resources=cpu_intensive_resources,
        execution_timeout=timedelta(seconds=300),
    )

    # Task: GPU Task 1 (KubernetesPodOperator)
    gpu_task_1 = KubernetesPodOperator(
        task_id='gpu_task_1',
        name='gpu-task-1',
        namespace='default',
        image='your-gpu-image:latest',
        cmds=["python", "-c", "import time; time.sleep(5); print('GPU task 1 completed')"],
        node_selector={'gpu': 'true'},
        container_resources=gpu_resources,
        execution_timeout=timedelta(seconds=300),
    )

    # Task: GPU Task 2 (KubernetesPodOperator)
    gpu_task_2 = KubernetesPodOperator(
        task_id='gpu_task_2',
        name='gpu-task-2',
        namespace='default',
        image='your-gpu-image:latest',
        cmds=["python", "-c", "import time; time.sleep(5); print('GPU task 2 completed')"],
        node_selector={'gpu': 'true'},
        container_resources=gpu_resources,
        execution_timeout=timedelta(seconds=300),
    )

    # Task: CPU Check Task (PythonOperator)
    def cpu_check():
        # Basic mathematical check for CPU
        result = sum([math.sqrt(i) for i in range(1, 10000)])
        print(f"CPU check result: {result}")

    cpu_check_task = PythonOperator(
        task_id='cpu_check_task',
        python_callable=cpu_check,
    )

    # Task: GPU Check Task (PythonOperator)
    def gpu_check():
        # Simple check using random number generation
        result = sum([random.random() for _ in range(1000000)])
        print(f"GPU check result: {result}")

    gpu_check_task = PythonOperator(
        task_id='gpu_check_task',
        python_callable=gpu_check,
    )

    # Task: Send Telegram Report (PythonOperator)
    def send_hourly_report():
        # Get bot token, chat_id, and message_thread_id from Airflow Variables
        bot_token = Variable.get('telegram_bot_token')
        chat_id = Variable.get('telegram_chat_id')
        message_thread_id = Variable.get('telegram_message_thread_id', default_var=None)

        # Load Kubernetes configuration
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        # Get node information
        try:
            v1 = client.CoreV1Api()
            nodes = v1.list_node()

            total_cpus = 0
            total_gpus = 0
            nodes_info = []

            for node in nodes.items:
                allocatable = node.status.allocatable
                cpu = allocatable.get('cpu')
                gpu = allocatable.get('nvidia.com/gpu', '0')
                memory = allocatable.get('memory')

                if cpu.endswith('m'):
                    cpu_count = float(cpu.rstrip('m')) / 1000
                else:
                    cpu_count = float(cpu)

                gpu_count = int(gpu)
                total_cpus += cpu_count
                total_gpus += gpu_count

                nodes_info.append({
                    'name': node.metadata.name,
                    'cpu': cpu_count,
                    'gpu': gpu_count,
                    'memory': memory
                })

            # Format message
            message = f"""
    🐱 *Ежечасный Отчёт Кластера* 🐱

    📊 *Доступные ресурсы для обучения:*
    - **CPU:** {total_cpus} ядра
    - **GPU:** {total_gpus} GPU

    🖥️ *Детали по нодам:*
    """
            for node in nodes_info:
                message += f"""
    - *Нода:* {node['name']}
      - CPU: {node['cpu']} ядра
      - GPU: {node['gpu']} GPU
      - Память: {node['memory']}
    """

            # Send message to Telegram
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown"
            }
            if message_thread_id:
                payload["message_thread_id"] = message_thread_id

            response = requests.post(url, data=payload)
            response.raise_for_status()
            print(f"Ежечасный отчёт успешно отправлен: {response.json()}")

        except Exception as e:
            print(f"Ошибка при отправке отчёта: {e}")
            raise

    send_report_task = PythonOperator(
        task_id='send_hourly_telegram_report',
        python_callable=send_hourly_report,
    )

    # Define task dependencies
    cpu_intensive_task >> [gpu_task_1, gpu_task_2] >> cpu_check_task >> gpu_check_task >> send_report_task
