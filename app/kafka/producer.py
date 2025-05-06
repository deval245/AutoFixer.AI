from aiokafka import AIOKafkaProducer
import asyncio
import json

KAFKA_TOPIC = "log-analyzer-topic"
KAFKA_BOOTSTRAP = "kafka:9092"

producer = None

async def start_kafka_producer():
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()

async def push_to_kafka(log_data: str):
    if producer is None:
        await start_kafka_producer()
    await producer.send_and_wait(KAFKA_TOPIC, log_data.encode("utf-8"))
