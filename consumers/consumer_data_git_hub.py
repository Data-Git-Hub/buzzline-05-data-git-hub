# producers/producer_much_ado.py
"""
Emit random (author, message) pairs built from Much Ado reference file.
Now with real sentiment via VADER (fallback to a tiny lexicon if missing).
"""

from __future__ import annotations

import json
import os
import pathlib
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional

from kafka import KafkaProducer

import utils.utils_config as config
from utils.utils_logger import logger
from utils.utils_producer import verify_services, create_kafka_topic

# ---------- paths (same logic you already had) ----------
ENV_REF = os.getenv("MUCH_ADO_PATH")
REF_CANDIDATES: List[pathlib.Path] = []
if ENV_REF:
    REF_CANDIDATES.append(pathlib.Path(ENV_REF))
REF_CANDIDATES.extend([
    pathlib.Path(r"C:\Projects\files\buzzline-05-data-git-hub\much_ado_excerpt.json"),
    pathlib.Path(r"C:\Projects\files\buzzline-05-data-git-hub\much_ado_except.json"),
    pathlib.Path(r"C:\Projects\files\buzzline-05-data-git-hub\much_ado_execpt.json"),
])
_PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
for _name in ["much_ado_excerpt.json", "much_ado_except.json", "much_ado_execpt.json"]:
    REF_CANDIDATES.append(_PROJECT_ROOT / "files" / _name)
    REF_CANDIDATES.append(_PROJECT_ROOT / _name)

# ---------- sentiment ----------
_VADER = None

def get_sentiment_analyzer():
    """Try to load VADER once; else return None."""
    global _VADER
    if _VADER is not None:
        return _VADER
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        _VADER = SentimentIntensityAnalyzer()
    except Exception as e:
        logger.warning(f"[producer_much_ado] VADER not available; using fallback lexicon. ({e})")
        _VADER = None
    return _VADER

_POS_WORDS = {
    "love","joy","happy","worthy","honour","honor","brave","valiant","good","sweet",
    "kind","pleasant","merry","praise","grace","fair","fine","noble","virtue","gentle",
}
_NEG_WORDS = {
    "kill","murder","villain","traitor","hate","anger","wrath","spite","malice",
    "curse","false","deceive","poison","rage","sad","tears","mock","scorn","lie","ill",
}

def assess_sentiment(text: str) -> float:
    """
    Return sentiment in [0,1].
    - Prefer VADER compound score (-1..1) mapped to 0..1.
    - Fallback: tiny bag-of-words score.
    """
    if not text:
        return 0.5
    analyzer = get_sentiment_analyzer()
    if analyzer:
        comp = analyzer.polarity_scores(text)["compound"]  # -1..1
        return round((comp + 1.0) / 2.0, 2)                # -> 0..1
    # Fallback heuristic
    toks = [t.lower() for t in text.split()]
    pos = sum(t in _POS_WORDS for t in toks)
    neg = sum(t in _NEG_WORDS for t in toks)
    score = 0.5
    if pos or neg:
        score = 0.5 + (pos - neg) / max(2, (pos + neg) * 2)  # dampen
    return round(min(1.0, max(0.0, score)), 2)

# ---------- data loading ----------
def _find_first_existing(paths: List[pathlib.Path]) -> Optional[pathlib.Path]:
    for p in paths:
        if p.exists():
            return p
    return None

def load_much_ado() -> List[Dict[str, str]]:
    ref = _find_first_existing(REF_CANDIDATES)
    if not ref:
        logger.error("[producer_much_ado] Could not find much_ado_excerpt JSON in any known location.")
        logger.error("[producer_much_ado] Paths tried:")
        for p in REF_CANDIDATES:
            logger.error(f"  - {p}")
        return []
    try:
        return json.loads(ref.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"[producer_much_ado] Failed reading reference file {ref}: {e}")
        return []

def build_pools(data: List[Dict[str, str]]) -> Tuple[List[str], List[str]]:
    authors = []
    messages = []
    for item in data:
        a = str(item.get("author", "")).strip()
        m = str(item.get("message", "")).strip()
        if a:
            authors.append(a)
        if m:
            messages.append(m)
    return authors, messages

# ---------- emitters ----------
def emit_to_file(message: Dict[str, Any], path: pathlib.Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(message, ensure_ascii=False) + "\n")

def main() -> None:
    logger.info("Starting Much Ado Producer (random author + random message).")
    logger.info("Use Ctrl+C to stop.")

    interval = config.get_message_interval_seconds_as_int()
    topic = config.get_kafka_topic()
    kafka_server = config.get_kafka_broker_address()
    live_data_path = config.get_live_data_path()

    # Reset file sink
    try:
        if live_data_path.exists():
            live_data_path.unlink()
        logger.info("Deleted existing live data file.")
    except Exception:
        pass

    # Load data
    data = load_much_ado()
    if not data:
        logger.error("[producer_much_ado] No data to emit. Exiting.")
        return

    authors, messages = build_pools(data)
    if not authors or not messages:
        logger.error("[producer_much_ado] Reference file didn’t yield authors/messages. Exiting.")
        return

    # Kafka (soft)
    producer = None
    try:
        if verify_services(strict=False):
            producer = KafkaProducer(bootstrap_servers=kafka_server)
            try:
                create_kafka_topic(topic)
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"[producer_much_ado] Kafka setup warning: {e}")
        producer = None

    try:
        while True:
            author = random.choice(authors)
            message = random.choice(messages)
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # compute real sentiment
            sentiment = assess_sentiment(message)

            # crude category/keyword_mentioned to keep schema stable
            keyword = "other"
            lower = message.lower()
            if any(w in lower for w in ("kill","murder","poison")):
                keyword = "violence"
            elif any(w in lower for w in ("love","joy","happy")):
                keyword = "affection"
            elif any(w in lower for w in ("honour","noble","virtue","grace")):
                keyword = "virtue"

            out = {
                "message": message,
                "author": author,
                "timestamp": ts,
                "category": "literature",
                "sentiment": sentiment,              # <-- now real
                "keyword_mentioned": keyword,
                "message_length": len(message),
            }

            # file sink (JSONL)
            emit_to_file(out, live_data_path)
            # kafka sink
            if producer:
                payload = json.dumps(out, ensure_ascii=False).encode("utf-8")
                producer.send(topic, value=payload)

            logger.info(out)
            time.sleep(interval)

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    finally:
        try:
            if producer:
                producer.flush(timeout=5)
                producer.close()
        except Exception:
            pass
        logger.info("Producer shutting down.")

if __name__ == "__main__":
    main()
