from aiokafka import AIOKafkaConsumer
import asyncio
from app.services.llm_handler import analyze_log

KAFKA_TOPIC = "log-analyzer-topic"
KAFKA_BOOTSTRAP = "kafka:9092"
GROUP_ID = "log-consumer-group"

async def consume_logs():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset='earliest'
    )
    await consumer.start()
    try:
        async for msg in consumer:
            log_content = msg.value.decode("utf-8")
            result = await analyze_log(log_content)
            print("🔥 LLM Output:", result)
            # TODO: Save result to DB or Redis if needed
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume_logs())
