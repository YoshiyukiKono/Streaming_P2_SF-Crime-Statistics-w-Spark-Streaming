import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

from confluent_kafka.admin import AdminClient

def topic_exists(topic):
    """Checks if the given topic exists in Kafka"""
    client = AdminClient({"bootstrap.servers": "PLAINTEXT://localhost:9092"})
    topic_metadata = client.list_topics(timeout=5)
    return topic in set(t.topic for t in iter(topic_metadata.topics.values()))

class TopicModel:
    """Defines the topic model"""

    def __init__(self):
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming topic model data"""
        value = message.value()
        logger.info("value: %s", value)
        self.temperature = value.get("temperature")
        self.status = value.get("status")

class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        self.broker_properties = {
            "bootstrap.servers": "localhost:9092",
            "group.id": "udacity",
            "auto.offset.reset": "earliest" if offset_earliest else "latest"
        }

        self.consumer = Consumer(self.broker_properties)
        logger.info("__init__ - Consumer was created")

        logger.info("Consumer will subscribe - %s", self.topic_name_pattern)
        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        logger.info("on_assign - self.topic_name_pattern: %s", self.topic_name_pattern)
        logger.info("on_assign - partitions: %s", partitions)
        logger.info("on_assign - self.consumer: %s", self.consumer)

        for partition in partitions:
            logger.info("on_assign - partition: %s", partition)
            partition.offset = OFFSET_BEGINNING
    
        logger.info("BEFORE partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)
        logger.info("AFTER partitions assigned for %s", self.topic_name_pattern)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        message = self.consumer.poll(1.0)
        if message is None:
            logger.info("no message received by consumer: %s", self.topic_name_pattern)
            return 0
        elif message.error() is not None:
            logger.info(f"error from consumer {message.error()}")
            return 0
        else:
            logger.info(f"consumed message {message.key()}: {message.value()}")
            self.message_handler(message)
            return 1


    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()


def run_server():
    """Begins Kafka consumption"""
    if topic_exists("police-department-calls-for-service") is False:
        print(
            "Ensure that the KSQL Command has run successfully before running the web server!"
        )
        exit(1)


    topic_model = TopicModel()

    # Build kafka consumers
    consumer = KafkaConsumer(
                "police-department-calls-for-service",
                topic_model.process_message,
                offset_earliest=True,
              )

    try:
        consumer.consume

    except KeyboardInterrupt as e:
        logger.info("shutting down server")
        consumer.close()


if __name__ == "__main__":
    run_server()