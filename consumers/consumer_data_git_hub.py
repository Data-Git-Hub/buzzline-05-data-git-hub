"""
consumer_data_git_hub.py

Custom consumer that:
- Reads from Kafka topic defined in .env (via utils_config).
- For EACH message, checks whether (author, message) appears in a reference JSON file.
- Flags presence of “keywords” loaded from an external file.
- Stores one processed result per message into SQLite (data/buzz.sqlite) in table author_validation.

Reference paths (Windows):
  Much Ado file:
    C:\Projects\files\buzzline-05-data-git-hub\much_ado_except.json
  Flag words file (first existing used):
    C:\Projects\files\buzzline-05-data-git-hub\flag_words
    C:\Projects\files\buzzline-05-data-git-hub\flag_words.txt
    C:\Projects\files\buzzline-05-data-git-hub\flag_words.json
"""

from __future__ import annotations

# stdlib
import json
import pathlib
import sqlite3
import sys
from typing import Any, Mapping, Set, Tuple, List

# externals
from kafka import KafkaConsumer  # kafka-python-ng

# locals
import utils.utils_config as config
from utils.utils_logger import logger
from utils.utils_consumer import create_kafka_consumer


# ----------------------------
# Absolute reference paths (Windows)
# ----------------------------
REF_FILE_CANDIDATES: List[pathlib.Path] = [
    pathlib.Path(r"C:\Projects\files\buzzline-05-data-git-hub\much_ado_except.json"),
    pathlib.Path(r"C:\Projects\files\buzzline-05-data-git-hub\much_ado_execpt.json"),  # fallback if typo exists
]

FLAG_WORD_CANDIDATES: List[pathlib.Path] = [
    pathlib.Path(r"C:\Projects\files\buzzline-05-data-git-hub\flag_words"),
    pathlib.Path(r"C:\Projects\files\buzzline-05-data-git-hub\flag_words.txt"),
    pathlib.Path(r"C:\Projects\files\buzzline-05-data-git-hub\flag_words.json"),
]


# ----------------------------
# SQLite table
# ----------------------------
TABLE_SQL = """
CREATE TABLE IF NOT EXISTS author_validation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    processed_at TEXT DEFAULT (datetime('now')),
    src_timestamp TEXT,           -- original message timestamp
    author TEXT,
    message TEXT,
    category TEXT,
    sentiment REAL,
    message_length INTEGER,

    author_message_valid INTEGER, -- 1 if (author,message) exists in reference set, else 0
    keyword_hit_count INTEGER,    -- count of keywords present
    keywords_hit TEXT             -- comma-separated list of keywords hit
);
"""


def init_db(db_path: pathlib.Path) -> None:
    """Ensure target table exists."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(TABLE_SQL)
        conn.commit()
    logger.info("[consumer_data_git_hub] SQLite ready at %s", db_path)


def insert_row(db_path: pathlib.Path, row: Mapping[str, Any]) -> None:
    """Insert one processed row."""
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO author_validation (
                src_timestamp, author, message, category, sentiment, message_length,
                author_message_valid, keyword_hit_count, keywords_hit
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                row.get("src_timestamp"),
                row.get("author"),
                row.get("message"),
                row.get("category"),
                float(row.get("sentiment", 0.0)),
                int(row.get("message_length", 0)),
                1 if row.get("author_message_valid") else 0,
                int(row.get("keyword_hit_count", 0)),
                row.get("keywords_hit", ""),
            ),
        )
        conn.commit()
    logger.info("[consumer_data_git_hub] Inserted one processed row.")


# ----------------------------
# Loaders: reference pairs & keywords
# ----------------------------
def load_reference_pairs() -> Set[Tuple[str, str]]:
    """
    Load (author,message) pairs from the first existing REF_FILE_CANDIDATE.
    File is expected to be a JSON array of { "message": "...", "author": "..." }.
    """
    for p in REF_FILE_CANDIDATES:
        if p.exists():
            logger.info("[consumer_data_git_hub] Using reference file: %s", p)
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
                pairs: Set[Tuple[str, str]] = set()
                for item in data:
                    author = str(item.get("author", "")).strip().lower()
                    message = str(item.get("message", "")).strip()
                    if author and message:
                        pairs.add((author, message))
                logger.info("[consumer_data_git_hub] Loaded %d ref pairs.", len(pairs))
                return pairs
            except Exception as e:
                logger.error("[consumer_data_git_hub] Failed to parse %s: %s", p, e)
                break
    logger.warning("[consumer_data_git_hub] No reference file found; author validation will be 0.")
    return set()


def load_flag_words() -> Set[str]:
    """
    Load keywords from the first existing FLAG_WORD_CANDIDATE.
    - If file ends with .json: expects a JSON list of strings.
    - Else: treats as text; supports newline- or comma-separated words.
    """
    for p in FLAG_WORD_CANDIDATES:
        if p.exists():
            logger.info("[consumer_data_git_hub] Using flag words file: %s", p)
            try:
                if p.suffix.lower() == ".json":
                    arr = json.loads(p.read_text(encoding="utf-8"))
                    words = {str(w).strip().lower() for w in arr if str(w).strip()}
                else:
                    raw = p.read_text(encoding="utf-8")
                    parts = []
                    for line in raw.splitlines():
                        parts.extend([seg.strip() for seg in line.replace(",", "\n").splitlines()])
                    words = {w.lower() for w in parts if w}
                logger.info("[consumer_data_git_hub] Loaded %d flag words.", len(words))
                return words
            except Exception as e:
                logger.error("[consumer_data_git_hub] Failed to load flag words from %s: %s", p, e)
                break

    logger.warning("[consumer_data_git_hub] No flag words file found; keyword hits will be 0.")
    return set()


# ----------------------------
# Processing
# ----------------------------
def process_message(msg: Mapping[str, Any], ref_pairs: Set[Tuple[str, str]], keywords: Set[str]) -> Mapping[str, Any]:
    """
    Build one processed row:
      - Validate (author,message) against reference pairs
      - Count hits for provided keywords (case-insensitive substring matches)
    """
    try:
        raw_message = str(msg.get("message", "")).strip()
        raw_author  = str(msg.get("author", "")).strip()
        author_norm = raw_author.lower()

        author_message_valid = (author_norm, raw_message) in ref_pairs if ref_pairs else False

        m_lc = raw_message.lower()
        hits = sorted([w for w in keywords if w in m_lc]) if keywords else []
        keyword_hit_count = len(hits)
        keywords_hit = ",".join(hits)

        row = {
            "src_timestamp": msg.get("timestamp"),
            "author": raw_author,
            "message": raw_message,
            "category": msg.get("category"),
            "sentiment": msg.get("sentiment", 0.0),
            "message_length": msg.get("message_length", 0),
            "author_message_valid": author_message_valid,
            "keyword_hit_count": keyword_hit_count,
            "keywords_hit": keywords_hit,
        }
        logger.info("[consumer_data_git_hub] Processed row: %s", row)
        return row
    except Exception as e:
        logger.error("[consumer_data_git_hub] process_message failed: %s", e)
        return {}


# ----------------------------
# Kafka poll loop (Windows-friendly)
# ----------------------------
def run_from_kafka() -> None:
    logger.info("[consumer_data_git_hub] Starting (Kafka mode).")

    # Read config for topic/broker/db
    topic: str = config.get_kafka_topic()
    group_id: str = config.get_kafka_consumer_group_id()
    sqlite_path: pathlib.Path = config.get_sqlite_path()

    # DB table ready
    init_db(sqlite_path)

    # Load reference & keywords
    ref_pairs = load_reference_pairs()
    keywords = load_flag_words()

    # Create consumer
    consumer: KafkaConsumer = create_kafka_consumer(
        topic,
        group_id,
        value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
    )

    if consumer is None:
        logger.error("[consumer_data_git_hub] Could not create consumer; exiting.")
        sys.exit(20)

    logger.info("[consumer_data_git_hub] Polling for messages...")
    max_restarts = 3
    restarts = 0

    import time as _time

    while True:
        try:
            records = consumer.poll(timeout_ms=1000, max_records=500)
            if not records:
                continue
            for _tp, msgs in records.items():
                for msg in msgs:
                    row = process_message(msg.value, ref_pairs, keywords)
                    if row:
                        insert_row(sqlite_path, row)

        except KeyboardInterrupt:
            logger.warning("[consumer_data_git_hub] Interrupted by user.")
            break
        except Exception as e:
            logger.error("[consumer_data_git_hub] Poll loop error: %s", e)
            try:
                consumer.close()
            except Exception:
                pass
            if restarts >= max_restarts:
                logger.error("[consumer_data_git_hub] Max restarts exceeded; exiting.")
                raise
            restarts += 1
            logger.warning("[consumer_data_git_hub] Recreating consumer (attempt %d/%d) after 1s...", restarts, max_restarts)
            _time.sleep(1.0)
            consumer = create_kafka_consumer(
                topic,
                group_id,
                value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
            )

    try:
        consumer.close()
    except Exception:
        pass
    logger.info("[consumer_data_git_hub] Shutdown complete.")


def main() -> None:
    # Default: Kafka mode
    run_from_kafka()


if __name__ == "__main__":
    main()
