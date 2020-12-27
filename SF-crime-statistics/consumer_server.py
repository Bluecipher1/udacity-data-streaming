import asyncio

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient

TOPIC_NAME = "com.udacity.crimestatistics"
BROKER_URL = "PLAINTEXT://localhost:9092"

async def consume(broker_url, topic_name):
    c = Consumer({"bootstrap.servers": broker_url, "group.id": "0"})
    c.subscribe([topic_name])

    while True:
        messages = c.consume(5, 1.0)
        print(f"Received {len(messages)} messages")
        for message in messages:
            if message is None:
                continue
            elif message.error() is not None:
                continue
            else:
                print(f"{message.key()} => {message.value()}")
            await asyncio.sleep(0.01)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    try:
        asyncio.run(consume(BROKER_URL, TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")

if __name__ == "__main__":
    main()