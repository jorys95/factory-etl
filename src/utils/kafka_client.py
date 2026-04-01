import os
import json
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS")
GROUP_ID = os.getenv("KAFKA_GROUP_ID")
raw_topic = os.getenv("KAFKA_RAW_TOPIC")
rich_topic = os.getenv("KAFKA_RICH_TOPIC")


def create_raw_consumer():
    if not KAFKA_BROKERS:
        raise ValueError("KAFKA_BROKERS environment variable must be set")
    if not raw_topic:
        raise ValueError("KAFKA_RAW_TOPIC environment variable must be set")
    if not GROUP_ID:
        raise ValueError("KAFKA_GROUP_ID environment variable must be set")
    return KafkaConsumer(
        raw_topic,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True
    )

def create_producer():
    if not KAFKA_BROKERS:
        raise ValueError("KAFKA_BROKERS environment variable must be set")
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

_producer = None

def get_producer():
    global _producer
    if _producer is None:
        _producer = create_producer()
    return _producer

def send_rich_event(event):
    producer = get_producer()
    producer.send(
            topic=rich_topic,
            value=event
        )
    producer.flush()
