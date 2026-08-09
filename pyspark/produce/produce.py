import os
import time
import json
import random

from datetime import datetime

from kafka import KafkaProducer

# Broker, topic and volume come from the environment. See .env.example.
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")
SPARK_TOPIC = os.getenv("SPARK_TOPIC", "stock_json_topic_spark")
PRODUCE_COUNT = int(os.getenv("PRODUCE_COUNT", "20000"))
PRODUCE_INTERVAL_SECONDS = float(os.getenv("PRODUCE_INTERVAL_SECONDS", "1"))

def get_json_data():

    stock = {
        'event_time': datetime.now().isoformat(),
        'ticker': random.choice(['AAPL', 'AMZN', 'MSFT', 'INTC', 'TBV']),
        'price': round(random.random() * 100, 2)
    }
    return json.dumps(stock) 

def main():
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS.split(','))

    for _ in range(PRODUCE_COUNT):
        json_data = get_json_data()
        producer.send(SPARK_TOPIC, bytes(f'{json_data}','UTF-8'))
        print(f"Data is sent: {json_data}")
        time.sleep(PRODUCE_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()