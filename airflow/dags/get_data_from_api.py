import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Orlando',
    'start_date': datetime(2024, 7, 10, 12)
}


def get_data():
    import requests

    url = "https://api.coincap.io/v2/rates/bitcoin"

    bearer_token = "38fa7654-c493-4d14-9f8b-5aed13ea98cf"

    headers = {"Authorization": f"Bearer {bearer_token}"}
    payload = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    response = response.json()

    return response


def format_data(response):
    data = {'symbol': response['data']['symbol'],
            'price': response['data']['rateUsd'],
            'timestamp': response['timestamp']}

    return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'],
                             max_block_ms=5000)

    while True:
        try:
            data = get_data()
            formatted_data = format_data(data)
            producer.send('bitcoin_price', json.dumps(formatted_data).encode('utf-8'))
            time.sleep(5)
        except Exception as e:
            logging.error(f'An error occurred: {e}')


with DAG('get_btc_data',
         default_args=default_args,
         schedule='*/1 * * * *',
         catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
