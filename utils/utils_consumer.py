"""
utils_consumer.py - common functions used by consumers.

Consumers subscribe to a topic and read messages from the Kafka topic.
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
from typing import Optional, Callable

# Import external packages
from kafka import KafkaConsumer

# Import functions from local modules
from utils.utils_logger import logger
from .utils_producer import get_kafka_broker_address

#####################################
# Default Configurations
#####################################

DEFAULT_CONSUMER_GROUP = "test_group"


#####################################
# Helper Functions
#####################################


def create_kafka_consumer(
    topic_provided: Optional[str] = None,
    group_id_provided: Optional[str] = None,
    value_deserializer_provided: Optional[Callable[[bytes], str]] = None,
):
    """
    Create and return a Kafka consumer instance.

    Args:
        topic_provided (str, required): The Kafka topic to subscribe to.
            This must be provided: a consumer cannot run without a topic.
        group_id_provided (str, optional): The consumer group ID.
            Defaults to test_group if not provided.
        value_deserializer_provided (callable, optional): Function to deserialize message values.

    Returns:
        KafkaConsumer: Configured Kafka consumer instance.

    Raises:
        ValueError: If no topic is provided.
    """
    kafka_broker = get_kafka_broker_address()

    topic = (topic_provided or "").strip()
    if not topic:
        msg = "Kafka consumer requires a topic. Provide topic_provided explicitly."
        logger.error(msg)
        raise ValueError(msg)

    consumer_group_id = (group_id_provided or DEFAULT_CONSUMER_GROUP).strip()

    value_deserializer: Callable[[bytes], str] = value_deserializer_provided or (
        lambda x: x.decode("utf-8")
    )

    logger.info(
        f"Creating Kafka consumer. Topic='{topic}' and group ID='{consumer_group_id}'."
    )
    logger.debug(f"Kafka broker: {kafka_broker}")

    try:
        consumer = KafkaConsumer(
            topic,
            group_id=consumer_group_id,
            value_deserializer=value_deserializer,
            bootstrap_servers=kafka_broker,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            request_timeout_ms=30000,
            session_timeout_ms=15000,
            heartbeat_interval_ms=3000,
        )
        logger.info("Kafka consumer created successfully.")
        return consumer
    except Exception as e:
        logger.error(f"Error creating Kafka consumer: {e}")
        raise
