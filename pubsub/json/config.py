"""Shared configuration for the pub/sub producer and consumer.

Values come from environment variables so the same code runs against a local
compose stack, a container on the compose network, or a remote broker.
Copy .env.example to .env and adjust.
"""
import os

from dotenv import load_dotenv

load_dotenv()

# Broker endpoint.
#   localhost:19092  -> from the host machine
#   redpanda:9092    -> from inside the compose network
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")

# Topic used by the plain confluent-kafka producer/consumer pair.
TOPIC = os.getenv("TOPIC", "stock_topic_test")

# Client/consumer group identifiers.
PRODUCER_CLIENT_ID = os.getenv("PRODUCER_CLIENT_ID", "python-producer")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "python-consumer")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "latest")
