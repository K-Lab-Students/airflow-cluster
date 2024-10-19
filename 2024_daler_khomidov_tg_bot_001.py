from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

def send_telegram_message():
    bot_token = '8128045612:AAH5dgKBSCpbUYMU4fD_qT2VHmoZejrUxew'  # Замените на ваш токен бота
    chat_id = '-4525994612'  # Замените на ваш chat_id
    message = "✅ *Airflow Status*\nAirflow is up and running!"

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }

    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
        print(f"Сообщение успешно отправлено: {response.json()}")
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error: {errh}")
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
    'owner': 'your-name',  # Замените на ваше имя или название команды
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Определение DAG
with DAG(
    dag_id='telegram_notification_minute',
    default_args=default_args,
    description='DAG для отправки сообщений через Telegram бот каждую минуту',
    schedule_interval=timedelta(minutes=1),  # Запуск каждую минуту
    start_date=datetime(2024, 4, 1),          # Установите желаемую дату начала
    catchup=False,                            # Отключает бэкафил
    tags=['telegram', 'notification', 'minute'],
) as dag:

    send_message = PythonOperator(
        task_id='send_telegram_message_task',
        python_callable=send_telegram_message,
    )

    send_message
