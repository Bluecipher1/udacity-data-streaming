"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

SCHEMA_REGISTRY_URL = "http://localhost:8081"
BROKER_URL = "PLAINTEXT://localhost:9092"

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            'schema.registry.url': SCHEMA_REGISTRY_URL,
            'bootstrap.servers': BROKER_URL,
            'group.id': self.topic_name,
        }

        self.producer = AvroProducer(
            config=self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)
            
    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        if self.producer.list_topics(timeout=5).topics.get(self.topic_name) is None:
            new_topic = NewTopic(
                topic=self.topic_name,
                num_partitions=self.num_partitions,
                replication_factor=self.num_replicas,
                config={
                    "cleanup.policy": "compact",
                    "compression.type": "lz4"
                }
            )
            client = AdminClient({"bootstrap.servers": BROKER_URL})
            result = client.create_topics([new_topic])
            
            for topic, future in result.items():
                try:
                    future.result()
                    logger.info(f"successfully created topic {self.topic_name}")
                except Exception as e:
                    logger.error(f"failed to create topic {self.topic_name}")
                    raise
        else:
            logger.info(f"topic {self.topic_name} already exists, skipping creation")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()
        logger.info("producer closed")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
