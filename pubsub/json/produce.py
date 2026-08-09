from confluent_kafka import Producer
from datetime import datetime
import random
import json
import uuid
from config import BOOTSTRAP_SERVERS, PRODUCER_CLIENT_ID, TOPIC

def produce():
    # Configure the Producer
    p = Producer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': PRODUCER_CLIENT_ID
    })

    # Produce a message
    try:
        while True:
            stock = {
                'event_time': datetime.now().isoformat(),
                'ticker': random.choice(['IDR', 'USD', 'SGD', 'JPY', 'EUR']),
                'price': round(random.random() * 100, 2)
            }
            p.produce(TOPIC, key=str(uuid.uuid4), value=json.dumps(stock), callback=delivery_report)
    except Exception as e:
        print(str(e))

    # Wait for any outstanding messages to be delivered
    p.flush()

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def main():
    produce()

if __name__ == "__main__":
    main()
    
