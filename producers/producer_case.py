"""
producer_case.py

Stream JSON data to any combination of sinks:
- File (JSONL)
- Kafka topic
- SQLite database
- DuckDB database

Each sink has its own one-line *emitter function* you can call inside the loop.
Comment out the ones you don't want.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

Environment variables are in utils/utils_config module.
"""

#####################################
# Import Modules
#####################################

# stdlib
import json
import os
import pathlib
import random
import sys
import time
from datetime import datetime
from typing import Mapping, Any

# external
from kafka import KafkaProducer  # kafka-python-ng

# local
import utils.utils_config as config
from utils.utils_producer import verify_services, create_kafka_topic
from utils.utils_logger import logger

# optional local emitters (single-responsibility helpers)
from utils.emitters import file_emitter, kafka_emitter, sqlite_emitter, duckdb_emitter


#####################################
# Stub Sentiment Analysis Function
#####################################

def assess_sentiment(text: str) -> float:
    """Stub for sentiment analysis; returns a random score in [0,1]."""
    return round(random.uniform(0, 1), 2)


#####################################
# Message Generator
#####################################

def generate_messages():
    """Yield JSON-able dicts forever."""
    ADJECTIVES = ["amazing", "funny", "boring", "exciting", "weird"]
    ACTIONS = ["found", "saw", "tried", "shared", "loved"]
    TOPICS = [
        "a movie", "a meme", "an app", "a trick", "a story",
        "Python", "JavaScript", "recipe", "travel", "game",
    ]
    AUTHORS = ["Alice", "Bob", "Charlie", "Eve"]
    KEYWORD_CATEGORIES = {
        "meme": "humor",
        "Python": "tech",
        "JavaScript": "tech",
        "recipe": "food",
        "travel": "travel",
        "movie": "entertainment",
        "game": "gaming",
    }

    while True:
        adjective = random.choice(ADJECTIVES)
        action = random.choice(ACTIONS)
        topic = random.choice(TOPICS)
        author = random.choice(AUTHORS)
        message_text = f"I just {action} {topic}! It was {adjective}."
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        keyword_mentioned = next((w for w in KEYWORD_CATEGORIES if w in topic), "other")
        category = KEYWORD_CATEGORIES.get(keyword_mentioned, "other")
        sentiment = assess_sentiment(message_text)

        yield {
            "message": message_text,
            "author": author,
            "timestamp": timestamp,
            "category": category,
            "sentiment": sentiment,
            "keyword_mentioned": keyword_mentioned,
            "message_length": len(message_text),
        }


#####################################
# Single-responsibility emitters (per-sink)
#####################################

def emit_to_file(message: Mapping[str, Any], *, path: pathlib.Path) -> bool:
    """Append one message to a JSONL file."""
    return file_emitter.emit_message(message, path=path)


def emit_to_kafka(
    message: Mapping[str, Any], *, producer: KafkaProducer, topic: str
) -> bool:
    """Publish one message to a Kafka topic."""
    return kafka_emitter.emit_message(message, producer=producer, topic=topic)


def emit_to_sqlite(message: Mapping[str, Any], *, db_path: pathlib.Path) -> bool:
    """Insert one message into SQLite."""
    return sqlite_emitter.emit_message(message, db_path=db_path)


def emit_to_duckdb(message: Mapping[str, Any], *, db_path: pathlib.Path) -> bool:
    """Insert one message into DuckDB."""
    return duckdb_emitter.emit_message(message, db_path=db_path)


#####################################
# Main
#####################################

def main() -> None:
    logger.info("Starting Producer to run continuously.")
    logger.info("Use Ctrl+C to stop.")

    # STEP 1. Read config
    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        topic: str = config.get_kafka_topic()
        kafka_server: str = config.get_kafka_broker_address()
        live_data_path: pathlib.Path = config.get_live_data_path()
        # Optional DB paths (fallbacks if not provided)
        sqlite_path: pathlib.Path = (
            config.get_sqlite_path() if hasattr(config, "get_sqlite_path")
            else pathlib.Path("data/buzz.sqlite")
        )
        duckdb_path: pathlib.Path = (
            config.get_duckdb_path() if hasattr(config, "get_duckdb_path")
            else pathlib.Path("data/buzz.duckdb")
        )
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    # STEP 2. Reset file sink (fresh run)
    try:
        if live_data_path.exists():
            live_data_path.unlink()
            logger.info("Deleted existing live data file.")
        os.makedirs(live_data_path.parent, exist_ok=True)
    except Exception as e:
        logger.error(f"ERROR: Failed to prep live data file: {e}")
        sys.exit(2)

    # STEP 3. (Optional) Setup Kafka
    producer = None
    try:
        # Soft-check: do not exit if Kafka is down
        if verify_services(strict=False):
            producer = KafkaProducer(bootstrap_servers=kafka_server)
            logger.info(f"Kafka producer connected to {kafka_server}")
            try:
                create_kafka_topic(topic)
                logger.info(f"Kafka topic '{topic}' is ready.")
            except Exception as e:
                logger.warning(f"WARNING: Topic create/verify failed ('{topic}'): {e}")
        else:
            logger.info("Kafka disabled for this run.")
    except Exception as e:
        logger.warning(f"WARNING: Kafka setup failed: {e}")
        producer = None

    # STEP 4. Emit loop — CALL ANY/ALL EMITTERS YOU WANT
    try:
        for message in generate_messages():
            logger.info(message)

            # --- File (JSONL) ---
            emit_to_file(message, path=live_data_path)

            # --- Kafka ---
            if producer is not None:
                emit_to_kafka(message, producer=producer, topic=topic)

            # --- SQLite ---
            # Uncomment to enable SQLite sink:
            # emit_to_sqlite(message, db_path=sqlite_path)

            # --- DuckDB ---
            # Uncomment to enable DuckDB sink:
            # emit_to_duckdb(message, db_path=duckdb_path)

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        if producer:
            try:
                producer.flush(timeout=5)
                producer.close()
                logger.info("Kafka producer closed.")
            except Exception:
                pass
        logger.info("Producer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
