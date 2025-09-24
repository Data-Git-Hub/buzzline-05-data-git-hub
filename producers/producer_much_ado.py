"""
producer_much_ado.py

Producer that streams messages built from Much Ado About Nothing:
- Loads the JSON file of {message, author} pairs
- Builds two lists: authors[] and messages[]
- For each emission, picks a RANDOM author and a RANDOM (independent) message
  so that (author, message) may or may not match the original play pairing.

Emits to:
- File (JSONL)    -> utils/emitters/file_emitter.py
- Kafka topic     -> utils/emitters/kafka_emitter.py   (topic from .env via utils_config)

Run:
  py -m producers.producer_much_ado
"""

from __future__ import annotations

# stdlib
import json
import os
import pathlib
import random
import sys
import time
from datetime import datetime
from typing import List, Dict, Any, Tuple

# external
from kafka import KafkaProducer  # kafka-python-ng

# local
import utils.utils_config as config
from utils.utils_logger import logger
from utils.utils_producer import verify_services, create_kafka_topic
from utils.emitters import file_emitter, kafka_emitter  # , sqlite_emitter, duckdb_emitter


# ---------------------------------------------------------------------------------------
# Build candidate locations for the Much Ado JSON
# ---------------------------------------------------------------------------------------

def _candidate_paths() -> List[pathlib.Path]:
    """All the places we’ll look, in priority order."""
    candidates: List[pathlib.Path] = []

    # 1) Environment variable (most explicit)
    env_path = os.getenv("MUCH_ADO_PATH")
    if env_path:
        candidates.append(pathlib.Path(env_path))

    # 2) Common absolute paths you used earlier (typo variants included)
    candidates.extend([
        pathlib.Path(r"C:\Projects\files\buzzline-05-data-git-hub\much_ado_excerpt.json"),
        pathlib.Path(r"C:\Projects\files\buzzline-05-data-git-hub\much_ado_except.json"),
        pathlib.Path(r"C:\Projects\files\buzzline-05-data-git-hub\much_ado_execpt.json"),
    ])

    # 3) Project-relative fallbacks:
    #    <PROJECT_ROOT>/files/<name>.json  (try a few spellings)
    project_root = pathlib.Path(__file__).resolve().parents[1]
    for name in [
        "much_ado_excerpt.json",
        "much_ado_except.json",
        "much_ado_execpt.json",
    ]:
        candidates.append(project_root / "files" / name)
        candidates.append(project_root / name)  # root-level fallback

    return candidates


def _load_much_ado() -> Tuple[List[str], List[str]]:
    """
    Load the Much Ado JSON file and return (authors, messages) lists.
    File is expected to be a JSON array of objects with keys "author" and "message".
    """
    tried: List[pathlib.Path] = []
    for p in _candidate_paths():
        tried.append(p)
        if p.exists():
            try:
                raw = p.read_text(encoding="utf-8")
                data = json.loads(raw)
                authors: List[str] = []
                messages: List[str] = []
                for item in data:
                    a = str(item.get("author", "")).strip()
                    m = str(item.get("message", "")).strip()
                    if a:
                        authors.append(a)
                    if m:
                        messages.append(m)
                if authors and messages:
                    logger.info(f"[producer_much_ado] Loaded {len(authors)} authors and {len(messages)} messages from {p}")
                    return authors, messages
                else:
                    logger.error(f"[producer_much_ado] File had no usable authors/messages: {p}")
                    break
            except Exception as e:
                logger.error(f"[producer_much_ado] Failed to parse JSON at {p}: {e}")
                break

    # If we get here, nothing worked
    logger.error("[producer_much_ado] Could not find much_ado JSON file. Paths tried:")
    for path in tried:
        logger.error(f"  - {path}")
    return [], []


def _random_author_message(authors: List[str], messages: List[str]) -> Dict[str, Any]:
    """
    Pick a RANDOM author and a RANDOM (independent) message.
    """
    author = random.choice(authors)
    message_text = random.choice(messages)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Minimal extras to fit the project schema
    msg = {
        "message": message_text,
        "author": author,
        "timestamp": timestamp,
        "category": "literature",
        "sentiment": 0.0,               # neutral
        "keyword_mentioned": "other",
        "message_length": len(message_text),
    }
    return msg


def emit_to_file(message: Dict[str, Any], *, path: pathlib.Path) -> bool:
    return file_emitter.emit_message(message, path=path)


def emit_to_kafka(message: Dict[str, Any], *, producer: KafkaProducer, topic: str) -> bool:
    return kafka_emitter.emit_message(message, producer=producer, topic=topic)


# ---------------------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------------------

def main() -> None:
    logger.info("Starting Much Ado Producer (random author + random message).")
    logger.info("Use Ctrl+C to stop.")

    # STEP 1. Config
    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        topic: str = config.get_kafka_topic()
        kafka_server: str = config.get_kafka_broker_address()
        live_data_path: pathlib.Path = config.get_live_data_path()
    except Exception as e:
        logger.error(f"[producer_much_ado] Failed to read environment variables: {e}")
        sys.exit(1)

    # STEP 2. Prepare live file sink
    try:
        if live_data_path.exists():
            live_data_path.unlink()
            logger.info("Deleted existing live data file.")
        os.makedirs(live_data_path.parent, exist_ok=True)
    except Exception as e:
        logger.error(f"[producer_much_ado] Failed to prep live data file: {e}")
        sys.exit(2)

    # STEP 3. Load reference corpus
    authors, messages = _load_much_ado()
    if not authors or not messages:
        logger.error("[producer_much_ado] No data to emit. Exiting.")
        sys.exit(3)

    # STEP 4. Optional Kafka setup
    producer = None
    try:
        if verify_services(strict=False):
            producer = KafkaProducer(bootstrap_servers=kafka_server)
            logger.info(f"[producer_much_ado] Kafka producer connected to {kafka_server}")
            try:
                create_kafka_topic(topic)
                logger.info(f"[producer_much_ado] Kafka topic '{topic}' is ready.")
            except Exception as e:
                logger.warning(f"[producer_much_ado] Topic create/verify failed ('{topic}'): {e}")
        else:
            logger.info("[producer_much_ado] Kafka disabled for this run.")
    except Exception as e:
        logger.warning(f"[producer_much_ado] Kafka setup failed: {e}")
        producer = None

    # STEP 5. Emit loop
    try:
        while True:
            message = _random_author_message(authors, messages)
            logger.info(message)

            # File sink
            emit_to_file(message, path=live_data_path)

            # Kafka sink
            if producer is not None:
                emit_to_kafka(message, producer=producer, topic=topic)

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("[producer_much_ado] Interrupted by user.")
    except Exception as e:
        logger.error(f"[producer_much_ado] Unexpected error: {e}")
    finally:
        if producer:
            try:
                producer.flush(timeout=5)
                producer.close()
                logger.info("[producer_much_ado] Kafka producer closed.")
            except Exception:
                pass
        logger.info("[producer_much_ado] Shutting down.")


if __name__ == "__main__":
    main()
