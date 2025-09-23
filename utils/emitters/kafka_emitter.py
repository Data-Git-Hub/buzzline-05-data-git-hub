"""
kafka_emitter.py

Emit messages to a Kafka topic.

A Kafka emitter integrates with a distributed streaming platform:
- Each message is serialized and published to a configured topic.
- Downstream consumers can subscribe independently.
- Useful for scaling and decoupling systems.

Use this for real-time streaming or when
integrating with other streaming analytics tools.
"""

import json
from typing import Mapping, Any

from utils.utils_logger import logger
from kafka import KafkaProducer


def emit_message(
    message: Mapping[str, Any],
    *,
    producer: KafkaProducer,  # Explicit type, works with postponed evaluation
    topic: str,
) -> bool:
    """
    Publish one message (dict-like) to a Kafka topic.

    Args:
        message:  Dict-like payload to send.
        producer: An initialized KafkaProducer instance.
        topic:    Target Kafka topic name.

    Returns:
        True on success, False on failure.
    """
    try:
        # Pretty Unicode support; still encoded as UTF-8 bytes for Kafka
        payload = json.dumps(message, ensure_ascii=False).encode("utf-8")
        producer.send(topic, value=payload)
        # Producer batches internally; callers can flush on shutdown if desired.
        logger.debug(f"[kafka_emitter] sent message to topic '{topic}'")
        return True
    except Exception as e:
        logger.error(f"[kafka_emitter] failed to send to '{topic}': {e}")
        return False
