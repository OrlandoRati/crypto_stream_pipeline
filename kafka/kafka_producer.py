from kafka import KafkaProducer
import requests
import json
import time

# Kafka configuration
kafka_topic = 'crypto-prices'
kafka_bootstrap_servers = 'localhost:9092'  # Update with your Kafka broker(s)

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Cryptocurrency API endpoint
crypto_api_url = 'https://api.coincap.io/v2/rates/bitcoin'

def fetch_crypto_data():
    try:
        bearer_token = "38fa7654-c493-4d14-9f8b-5aed13ea98cf"

        headers = {"Authorization": f"Bearer {bearer_token}"}
        payload = {}
        response = requests.get(url=crypto_api_url,
                                headers=headers,
                                data=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def format_data(response):
    data = {'symbol': response['data']['symbol'],
            'price': response['data']['rateUsd'],
            'timestamp': response['timestamp']}

    return data

def stream_data():
    while True:
        data = fetch_crypto_data()
        if data:
            formatted_data = format_data(data)
            producer.send(kafka_topic, formatted_data)
            print(f"Sent data to Kafka: {formatted_data}")
        time.sleep(5)

if __name__ == "__main__":
    stream_data()
