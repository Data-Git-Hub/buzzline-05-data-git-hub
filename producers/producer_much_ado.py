"""
producer_much_ado.py

Emits messages by randomly pairing an author and an (independent) message
from a Much Ado About Nothing excerpt JSON file.

File resolution order:
1) Environment variable MUCH_ADO_JSON (if set)
2) Absolute path: C:\Projects\buzzline-05-data-git-hub\files\much_ado_excerpt.json
3) Relative path: files/much_ado_excerpt.json
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
from typing import Any, Dict, List, Optional, Tuple

# external
from dotenv import load_dotenv
from kafka import KafkaProducer

# local
import utils.utils_config as config
from utils.utils_logger import logger
from utils.utils_producer import verify_services, create_kafka_topic

# -----------------------------------------------------------------------------
# Tiny lexicon sentiment (0..1), neutral ~0.5
# -----------------------------------------------------------------------------
_POS = {
    "joy", "happy", "happiness", "love", "sweet", "honour", "honor",
    "good", "worthy", "praise", "brave", "valiant", "merry", "glad",
    "grace", "welcome", "virtues", "comfort", "preparation", "prepare",
}
_NEG = {
    "sad", "sorrow", "tears", "ill", "bitter", "disease",
    "anger", "mad", "hang", "devil", "canker", "villain",
    "overthrow", "die", "hurt", "kill", "murder", "pestilence",
}

_PUNCT = ".,;:!?’'\"-()[]"

def tiny_sentiment(text: str) -> float:
    toks = [t.strip(_PUNCT).lower() for t in text.split()]
    pos = sum(1 for t in toks if t in _POS)
    neg = sum(1 for t in toks if t in _NEG)
    if pos == 0 and neg == 0:
        return 0.5
    score = 0.5 + (pos - neg) / (pos + neg) / 2  # map to ~0..1 around 0.5
    return max(0.0, min(1.0, round(score, 2)))


# -----------------------------------------------------------------------------
# File resolution (ONLY your real path + a relative fallback)
# -----------------------------------------------------------------------------
def _resolve_much_ado_json() -> Optional[pathlib.Path]:
    load_dotenv()  # allow .env overrides

    env_path = os.getenv("MUCH_ADO_JSON", "").strip()
    candidates: List[pathlib.Path] = []
    if env_path:
        candidates.append(pathlib.Path(env_path))

    # Your actual location
    candidates.append(pathlib.Path(r"C:\Projects\buzzline-05-data-git-hub\files\much_ado_excerpt.json"))
    # Relative fallback (repository-root)
    candidates.append(pathlib.Path("files/much_ado_excerpt.json"))

    tried = []
    for p in candidates:
        exists = p.exists()
        tried.append((str(p), exists))
        if exists:
            logger.info(f"[producer_much_ado] Using reference JSON: {p}")
            return p

    logger.error("[producer_much_ado] Could not find much_ado_excerpt JSON in any known location.")
    for path_str, exists in tried:
        logger.error(f"  tried: {path_str}  -> exists={exists}")
    return None


def _load_corpus(path: pathlib.Path) -> Tuple[List[str], List[str]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    authors, messages = [], []
    for item in data:
        a = str(item.get("author", "")).strip()
        m = str(item.get("message", "")).strip()
        if a:
            authors.append(a)
        if m:
            messages.append(m)
    return authors, messages


def _emit(
    payload: Dict[str, Any],
    *,
    live_path: pathlib.Path,
    producer: Optional[KafkaProducer],
    topic: str,
) -> None:
    # Append to JSONL file
    live_path.parent.mkdir(parents=True, exist_ok=True)
    with live_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    # Send to Kafka if available
    if producer is not None:
        try:
            producer.send(topic, value=json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        except Exception as e:
            logger.warning(f"[producer_much_ado] Kafka send failed (continuing file output): {e}")


def main() -> None:
    logger.info("Starting Much Ado Producer (random author + random message).")
    logger.info("Use Ctrl+C to stop.")

    # Config
    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        topic: str = config.get_kafka_topic()
        kafka_server: str = config.get_kafka_broker_address()
        live_data_path: pathlib.Path = config.get_live_data_path()
    except Exception as e:
        logger.error(f"[producer_much_ado] Config error: {e}")
        sys.exit(1)

    # Resolve corpus
    corpus_path = _resolve_much_ado_json()
    if not corpus_path:
        logger.error("[producer_much_ado] No data to emit. Exiting.")
        sys.exit(2)

    authors, messages = _load_corpus(corpus_path)
    if not authors or not messages:
        logger.error("[producer_much_ado] Corpus empty. Exiting.")
        sys.exit(3)

    # Fresh JSONL file
    try:
        if live_data_path.exists():
            live_data_path.unlink()
        logger.info("Deleted existing live data file.")
    except Exception as e:
        logger.warning(f"[producer_much_ado] Could not reset live file (continuing): {e}")

    # Kafka (soft)
    producer: Optional[KafkaProducer] = None
    try:
        if verify_services(strict=False):
            producer = KafkaProducer(bootstrap_servers=kafka_server)
            try:
                create_kafka_topic(topic)
            except Exception as te:
                logger.warning(f"[producer_much_ado] Topic create/verify failed ({topic}): {te}")
        else:
            logger.info("[producer_much_ado] Kafka disabled for this run (service not ready).")
    except Exception as e:
        logger.warning(f"[producer_much_ado] Kafka setup failed (file-only mode): {e}")
        producer = None

    # Emit loop
    try:
        while True:
            author = random.choice(authors)
            message = random.choice(messages)
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            payload = {
                "message": message,
                "author": author,
                "timestamp": ts,
                "category": "literature",
                "sentiment": tiny_sentiment(message),
                "keyword_mentioned": "none",  # keep schema; consumer does real keyword detection
                "message_length": len(message),
            }
            logger.info(payload)
            _emit(payload, live_path=live_data_path, producer=producer, topic=topic)
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"[producer_much_ado] Unexpected error: {e}")
    finally:
        if producer:
            try:
                producer.flush(timeout=5)
                producer.close()
            except Exception:
                pass
        logger.info("Producer shutting down.")


if __name__ == "__main__":
    main()
