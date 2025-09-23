"""
utils_producer.py - common functions used by producers.

Producers send messages to a Kafka topic.
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time
from typing import Callable, Optional, Any

# Import external packages
from dotenv import load_dotenv
from kafka import KafkaProducer, errors
from kafka.admin import (
    KafkaAdminClient,
    NewTopic,
)

# Import functions from local modules
from utils.utils_logger import logger


#####################################
# Default Configurations
#####################################

DEFAULT_KAFKA_BROKER_ADDRESS = "localhost:9092"

#####################################
# Helper Functions
#####################################


def get_kafka_broker_address():
    """Fetch Kafka broker address from environment or use default."""
    broker_address = os.getenv("KAFKA_BROKER_ADDRESS", "localhost:9092")
    logger.info(f"Kafka broker address: {broker_address}")
    return broker_address


#####################################
# Kafka Readiness Check
#####################################


def check_kafka_service_is_ready():
    """
    Check if Kafka is ready by connecting to the broker and fetching metadata.

    Returns:
        bool: True if Kafka is ready, False otherwise.
    """
    kafka_broker = get_kafka_broker_address()

    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)
        cluster_info: dict = admin_client.describe_cluster()
        logger.info(f"Kafka is ready. Brokers: {cluster_info}")
        admin_client.close()
        return True
    except errors.KafkaError as e:
        logger.error(f"Error checking Kafka: {e}")
        return False


#####################################
# Kafka Producer and Topic Management
#####################################


def verify_services(strict: bool = False) -> bool:
    """
    Check Kafka readiness. If strict=False, do not exit when unavailable.

    Returns:
        bool: True if Kafka is ready, False otherwise.
    """
    ready = check_kafka_service_is_ready()
    if ready:
        return True

    msg = "Kafka broker not available; continuing without Kafka."
    if strict:
        logger.error("Kafka broker is not ready. Please check your Kafka setup. Exiting...")
        sys.exit(2)
    else:
        logger.warning(msg)
        return False



def create_kafka_producer(
    value_serializer: Optional[Callable[[Any], bytes]] = None,
) -> Optional[KafkaProducer]:
    """
    Create and return a Kafka producer instance.

    Args:
        value_serializer (callable): A custom serializer for message values.
                                     Defaults to UTF-8 string encoding.

    Returns:
        KafkaProducer: Configured Kafka producer instance.
    """
    kafka_broker = get_kafka_broker_address()

    if value_serializer is None:

        def default_value_serializer(x: str) -> bytes:
            return x.encode("utf-8")  # Default to string serialization

        value_serializer = default_value_serializer

    try:
        logger.info(f"Connecting to Kafka broker at {kafka_broker}...")
        producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=value_serializer,
        )
        logger.info("Kafka producer successfully created.")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None


def _topic_exists(admin: KafkaAdminClient, topic_name: str) -> bool:
    try:
        return topic_name in set(admin.list_topics())
    except Exception:
        # If listing fails, assume it doesn't exist to avoid false positives
        return False


def _delete_topic_if_exists(admin: KafkaAdminClient, topic_name: str) -> None:
    """Delete topic if present and wait briefly for deletion to complete."""
    try:
        if _topic_exists(admin, topic_name):
            admin.delete_topics([topic_name])
            logger.info(f"Requested deletion of topic '{topic_name}'.")
            # Wait a short time for deletion to propagate
            deadline = time.time() + 10
            while time.time() < deadline:
                if not _topic_exists(admin, topic_name):
                    break
                time.sleep(0.2)
    except Exception as e:
        logger.warning(f"Ignoring topic deletion issue for '{topic_name}': {e}")


def create_kafka_topic(topic_name, group_id=None) -> None:
    """
    Create a fresh Kafka topic with the given name.
    If it already exists, delete and recreate it (simple reset; no retention tweaks).

    Args:
        topic_name (str): Name of the Kafka topic.
        group_id (str|None): Unused (kept for signature compatibility).
    """
    kafka_broker = get_kafka_broker_address()
    admin_client = None

    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)

        if _topic_exists(admin_client, topic_name):
            logger.info(f"Topic '{topic_name}' already exists. Recreating fresh...")
            _delete_topic_if_exists(admin_client, topic_name)

        new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])
        logger.info(f"Topic '{topic_name}' created successfully.")

    except Exception as e:
        logger.error(f"Error managing topic '{topic_name}': {e}")
        sys.exit(1)
    finally:
        if admin_client is not None:
            try:
                admin_client.close()
            except Exception:
                pass


def clear_kafka_topic(topic_name: str, group_id: Optional[str] = None):
    """
    Clear all messages in a Kafka topic by deleting and recreating it.
    This keeps the same function signature but uses a simpler, more reliable approach.

    Args:
        topic_name (str): Name of the Kafka topic.
        group_id (str, optional): Consumer group ID (not used in this simplified version).
    """
    kafka_broker = get_kafka_broker_address()
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)

    try:
        logger.info(f"Clearing topic '{topic_name}' by deleting and recreating it.")

        # Delete the topic if it exists
        if topic_name in admin_client.list_topics():
            admin_client.delete_topics([topic_name])
            logger.info(f"Deleted topic '{topic_name}'.")
            time.sleep(2)  # allow Kafka time to finish deletion

        # Recreate the topic
        new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])
        logger.info(f"Recreated topic '{topic_name}' successfully.")

    except Exception as e:
        logger.error(f"Error clearing topic '{topic_name}': {e}")
    finally:
        admin_client.close()


#####################################
# Main Function for Testing
#####################################


def main():
    """
    Main entry point.
    """

    logger.info("Starting utils_producer.py script...")
    logger.info("Loading environment variables from .env file...")
    load_dotenv()

    if not check_kafka_service_is_ready():
        logger.error("Kafka is not ready. Check .env file and ensure Kafka is running.")
        sys.exit(2)

    logger.info("All services are ready. Proceed with producer setup.")
    create_kafka_topic("test_topic", "default_group")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
