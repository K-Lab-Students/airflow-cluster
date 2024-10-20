from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests

# Default arguments for the DAG
default_args = {
    'owner': 'кошкодевочка',
    'start_date': datetime(2024, 10, 19, 22, 40),  # Установите желаемую дату и время начала
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Функция для отправки сообщения в Telegram
def send_airflow_alive_message():
    # Получение переменных Airflow
    bot_token = Variable.get('telegram_bot_token')
    chat_id = Variable.get('telegram_chat_id')

    # Формирование сообщения
    message = "🐱 *Airflow жив!* 🐱"

    # URL для отправки сообщения через Telegram API
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    # Параметры запроса
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }
       # URL для отправки сообщения
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    # Параметры запроса
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }
    message_thread_id = Variable.get('telegram_message_thread_id', default_var=None)

    # Добавление message_thread_id, если он задан
    if message_thread_id:
        payload["message_thread_id"] = message_thread_id

    try:
        # Отправка POST-запроса
        response = requests.post(url, data=payload)
        response.raise_for_status()  # Проверка на успешный статус
        print(f"Сообщение успешно отправлено: {response.json()}")
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP ошибка: {errh} - Ответ: {response.text}")
        raise
    except requests.exceptions.ConnectionError as errc:
        print(f"Ошибка соединения: {errc}")
        raise
    except requests.exceptions.Timeout as errt:
        print(f"Ошибка таймаута: {errt}")
        raise
    except requests.exceptions.RequestException as err:
        print(f"Ошибка запроса: {err}")
        raise

# Определение DAG
with DAG(
    dag_id='simple_airflow_alive_report',
    default_args=default_args,
    description='Простой DAG для отправки сообщения "Airflow жив" в Telegram',
    schedule_interval=timedelta(minutes=1),  # Запуск раз в час
    catchup=False,
    tags=['simple', 'telegram', 'health-check'],
) as dag:

    send_alive_message = PythonOperator(
        task_id='send_airflow_alive_message',
        python_callable=send_airflow_alive_message,
    )

    send_alive_message
