from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'кошкодевочка',
    'start_date': datetime(2024, 7, 19, 22, 40), 
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def send_airflow_alive_message():
    bot_token = "8128045612:AAENaeVPYa9YHCzJfk07aYCSmOLMEplaqAM" #Variable.get('telegram_bot_token')
    chat_id = "-1002095886585" #Variable.get('telegram_chat_id')
    utc_now = datetime.now()
    utc_plus_3 = utc_now + timedelta(hours=3)
    formatted_time = utc_plus_3.strftime("%Y-%m-%d %H:%M:%S")
    message = f"🐱 *Airflow жив!* 🐱\nОтчетное время (UTC+3): {formatted_time}"
    return message
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }
    message_thread_id = 15206 #Variable.get('telegram_message_thread_id', default_var=None)
    if message_thread_id:
        payload["message_thread_id"] = message_thread_id

    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()  
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

with DAG(
    dag_id='simple_airflow_alive_report',
    default_args=default_args,
    description='Простой DAG для отправки сообщения "Airflow жив" в Telegram',
    schedule_interval=timedelta(hours=1),  
    catchup=False,
    tags=['simple', 'telegram', 'health-check'],
) as dag:

    send_alive_message = PythonOperator(
        task_id='send_airflow_alive_message',
        python_callable=send_airflow_alive_message,
    )

    send_alive_message
