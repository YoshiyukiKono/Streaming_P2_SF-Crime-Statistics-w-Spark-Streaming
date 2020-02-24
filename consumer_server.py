import asyncio
from dataclasses import dataclass, field
import json
import random

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "police-department-calls-for-service"

def topic_exists(topic):
    """Checks if the given topic exists in Kafka"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    topic_metadata = client.list_topics(timeout=5)
    return topic in set(t.topic for t in iter(topic_metadata.topics.values()))

async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])

    while True:

        # Consume to grab 5 messages at a time and has a timeout.
        # https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.Consumer.consume
        print(f"start consuming")
        messages = c.consume(5, timeout=1.0)

        # Print the key and value of any message(s) consumed
        print(f"consumed {len(messages)} messages")
        for message in messages:
            print(f"consume message {message.key()}: {message.value()}")

        await asyncio.sleep(0.01)


async def run_consumer_task(topic_name):
    """Runs the Consumer tasks"""
    t2 = asyncio.create_task(consume(topic_name))
    await t2

def main():
    """Checks for topic and creates the topic if it does not exist"""
    if topic_exists(TOPIC_NAME) is False:
        print(
            f"{TOPIC_NAME} does not exist."
        )
        exit(1)

    try:
        asyncio.run(run_consumer_task(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()