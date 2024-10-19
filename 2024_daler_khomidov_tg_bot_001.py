from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kubernetes import client, config
import requests
from airflow.models import Variable

# Функция для получения информации о доступных нодах
def get_available_nodes():
    try:
        # Загрузка конфигурации Kubernetes
        config.load_incluster_config()  # Используйте, если DAG работает внутри кластера
        # config.load_kube_config()     # Используйте, если DAG работает локально с доступом к kubeconfig

        v1 = client.CoreV1Api()
        nodes = v1.list_node()

        available_cpus = 0
        available_gpus = 0
        available_nodes = []

        for node in nodes.items:
            # Получение ресурсов ноды
            allocatable = node.status.allocatable
            cpu = allocatable.get('cpu')
            memory = allocatable.get('memory')
            # Предполагаем, что GPU ресурсы имеют ключ 'nvidia.com/gpu'
            gpu = allocatable.get('nvidia.com/gpu', '0')

            # Преобразование ресурсов в числа
            if cpu.endswith('m'):
                cpu_count = float(cpu.rstrip('m')) / 1000  # Преобразование из мильядных
            else:
                cpu_count = float(cpu)
            gpu_count = int(gpu)

            # Суммируем доступные ресурсы
            available_cpus += cpu_count
            available_gpus += gpu_count

            available_nodes.append({
                'name': node.metadata.name,
                'cpu': cpu_count,
                'gpu': gpu_count,
                'memory': memory
            })

        return {
            'total_available_cpus': available_cpus,
            'total_available_gpus': available_gpus,
            'nodes': available_nodes
        }

    except Exception as e:
        print(f"Ошибка при получении информации о нодах: {e}")
        raise

# Функция для отправки сообщения в Telegram
def send_hourly_report():
    # Получение токена бота, chat_id и message_thread_id из Airflow Variables
    bot_token = Variable.get('telegram_bot_token')
    chat_id = Variable.get('telegram_chat_id')
    message_thread_id = Variable.get('telegram_message_thread_id', default_var=None)

    # Получение информации о нодах
    node_info = get_available_nodes()
    total_cpus = node_info['total_available_cpus']
    total_gpus = node_info['total_available_gpus']
    nodes = node_info['nodes']

    # Формирование сообщения
    message = f"""
🐱 *Ежечасный Отчёт Кластера* 🐱

📊 *Доступные ресурсы для обучения:*
- **CPU:** {total_cpus} ядра
- **GPU:** {total_gpus} GPU

🖥️ *Детали по нодам:*
"""
    for node in nodes:
        message += f"""
- *Нода:* {node['name']}
  - CPU: {node['cpu']} ядра
  - GPU: {node['gpu']} GPU
  - Память: {node['memory']}
"""

    # URL для отправки сообщения
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    # Параметры запроса
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }

    # Добавление message_thread_id, если он задан
    if message_thread_id:
        payload["message_thread_id"] = message_thread_id

    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
        print(f"Ежечасный отчёт успешно отправлен: {response.json()}")
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error: {errh} - Ответ: {response.text}")
        raise
    except requests.exceptions.ConnectionError as errc:
        print(f"Error Connecting: {errc}")
        raise
    except requests.exceptions.Timeout as errt:
        print(f"Timeout Error: {errt}")
        raise
    except requests.exceptions.RequestException as err:
        print(f"OOps: Something Else {err}")
        raise

# Default arguments для DAG
default_args = {
    'owner': 'кошкодевочка',            # Имя владельца DAG
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Определение DAG
with DAG(
    dag_id='telegram_hourly_report',
    default_args=default_args,
    description='DAG для отправки ежечасных отчётов о доступных кластерных нодах через Telegram бот "кошкодевочка"',
    schedule_interval=timedelta(minutes=1),  # Запуск раз в час
    start_date=datetime(2024, 10, 19, 18, 40),  # Установите желаемую дату и время начала
    catchup=False,
    tags=['telegram', 'report', 'hourly', 'cluster'],
) as dag:

    send_report = PythonOperator(
        task_id='send_hourly_telegram_report',
        python_callable=send_hourly_report,
    )

    send_report
