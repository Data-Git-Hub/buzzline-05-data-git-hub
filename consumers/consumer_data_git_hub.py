# consumers/consumer_data_git_hub.py
"""
Custom consumer (Kafka) that:
- Validates (author, message) pairs against a reference corpus (Much Ado).
- Flags messages that contain configured "flag words".
- Writes one processed row per message to SQLite (author_validation table).
- Emits "alerts" to console and to SQLite (alerts table) when policy matches.

Run:
  # terminal 1 (producer)
  py -m producers.producer_much_ado

  # terminal 2 (this consumer)
  py -m consumers.consumer_data_git_hub
"""

from __future__ import annotations

# stdlib
import json
import os
import re
import sqlite3
import pathlib
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

# external
from kafka import KafkaConsumer

# local
import utils.utils_config as config
from utils.utils_logger import logger
from utils.utils_consumer import create_kafka_consumer
from utils.utils_producer import verify_services


# ------------------------------------------------------------------------------------------
# Environment helpers / Policy
# ------------------------------------------------------------------------------------------

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default

# Alert policy:
# True  = require (author,message) to exist in reference set AND keyword hits >= ALERT_KEYWORD_MIN
# False = require non-empty author AND keyword hits >= ALERT_KEYWORD_MIN
ALERT_ON_VALID_AUTHOR_ONLY: bool = _env_bool("ALERT_ON_VALID_AUTHOR_ONLY", False)
ALERT_KEYWORD_MIN: int = _env_int("ALERT_KEYWORD_MIN", 1)


# ------------------------------------------------------------------------------------------
# Paths (use repo root /files)
# ------------------------------------------------------------------------------------------

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
FILES_DIR = PROJECT_ROOT / "files"

REF_FILE_CANDIDATES: List[pathlib.Path] = [
    FILES_DIR / "much_ado_excerpt.json",
    FILES_DIR / "much_ado_except.json",
    FILES_DIR / "much_ado_execpt.json",
]

FLAG_WORDS_FILE = FILES_DIR / "flag_words.txt"


# ------------------------------------------------------------------------------------------
# SQLite schema / helpers
# ------------------------------------------------------------------------------------------

AUTHOR_VALIDATION_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS author_validation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    src_timestamp TEXT,
    author TEXT,
    message TEXT,
    author_message_valid INTEGER,
    keyword_hit_count INTEGER,
    keywords_hit TEXT
);
"""

ALERTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    processed_at TEXT DEFAULT (datetime('now')),
    src_timestamp TEXT,
    author TEXT,
    message TEXT,
    keyword_hit_count INTEGER,
    keywords_hit TEXT
);
"""

def init_db(db_path: pathlib.Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(AUTHOR_VALIDATION_TABLE_SQL)
        conn.execute(ALERTS_TABLE_SQL)
        conn.commit()
    logger.info(f"[consumer_data_git_hub] SQLite ready at {db_path}")

def insert_row(db_path: pathlib.Path, row: Mapping[str, Any]) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO author_validation (
                src_timestamp, author, message, author_message_valid,
                keyword_hit_count, keywords_hit
            ) VALUES (?, ?, ?, ?, ?, ?);
            """,
            (
                row.get("src_timestamp"),
                row.get("author"),
                row.get("message"),
                int(row.get("author_message_valid", 0)),
                int(row.get("keyword_hit_count", 0)),
                row.get("keywords_hit", ""),
            ),
        )
        conn.commit()
    logger.info("[consumer_data_git_hub] Inserted one processed row.")

def insert_alert(db_path: pathlib.Path, row: Mapping[str, Any]) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO alerts (
                src_timestamp, author, message, keyword_hit_count, keywords_hit
            ) VALUES (?, ?, ?, ?, ?);
            """,
            (
                row.get("src_timestamp"),
                row.get("author"),
                row.get("message"),
                int(row.get("keyword_hit_count", 0)),
                row.get("keywords_hit", ""),
            ),
        )
        conn.commit()


def should_alert(row: Mapping[str, Any]) -> bool:
    """Return True if this row should generate an alert based on policy."""
    if int(row.get("keyword_hit_count", 0)) < ALERT_KEYWORD_MIN:
        return False
    if ALERT_ON_VALID_AUTHOR_ONLY:
        return bool(row.get("author_message_valid"))
    else:
        return bool(str(row.get("author", "")).strip())


# ------------------------------------------------------------------------------------------
# Reference corpus loader (Much Ado)
# ------------------------------------------------------------------------------------------

_TOKEN_RE = re.compile(r"[A-Za-z']+")

def _tokenize_lower(text: str) -> List[str]:
    return [t.lower() for t in _TOKEN_RE.findall(text or "")]

def load_reference_pairs_and_tokens() -> Tuple[Set[Tuple[str, str]], List[str]]:
    """
    Load the Much Ado reference JSON. Returns:
      - a set of (author_lower, message_stripped_lower)
      - a flat list of tokens from all messages (lowercased)
    """
    ref_path: Optional[pathlib.Path] = None
    for p in REF_FILE_CANDIDATES:
        if p.exists():
            ref_path = p
            break

    if not ref_path:
        logger.warning("[consumer_data_git_hub] No reference file found; author validation will be 0.")
        return set(), []

    try:
        raw = ref_path.read_text(encoding="utf-8")
        data = json.loads(raw)
        pairs: Set[Tuple[str, str]] = set()
        tokens: List[str] = []
        for item in data:
            author = str(item.get("author", "")).strip().lower()
            message = str(item.get("message", "")).strip().lower()
            if author and message:
                pairs.add((author, message))
                tokens.extend(_tokenize_lower(message))
        logger.info(f"[consumer_data_git_hub] Loaded reference pairs: {len(pairs)} from {ref_path}")
        return pairs, tokens
    except Exception as e:
        logger.error(f"[consumer_data_git_hub] Failed to read reference file: {e}")
        return set(), []


# ------------------------------------------------------------------------------------------
# Flag words loader (file-based only, per your request)
# ------------------------------------------------------------------------------------------

def load_flag_words() -> Set[str]:
    """
    Load flag words from files/flag_words.txt (one word per line).
    If missing, returns empty set and alerts won't fire.
    """
    if not FLAG_WORDS_FILE.exists():
        logger.warning("[consumer_data_git_hub] No flag words file found; keyword alerts will never trigger.")
        return set()
    try:
        words = {
            w.strip().lower()
            for w in FLAG_WORDS_FILE.read_text(encoding="utf-8").splitlines()
            if w.strip()
        }
        logger.info(f"[consumer_data_git_hub] Loaded {len(words)} flag words from {FLAG_WORDS_FILE}")
        return words
    except Exception as e:
        logger.error(f"[consumer_data_git_hub] Failed to read flag words file: {e}")
        return set()


# ------------------------------------------------------------------------------------------
# Per-message processing
# ------------------------------------------------------------------------------------------

def validate_author_message(
    author: str,
    message: str,
    ref_pairs: Set[Tuple[str, str]],
) -> bool:
    """Exact match on (author_lower, message_lower) against the reference set."""
    a = (author or "").strip().lower()
    m = (message or "").strip().lower()
    return (a, m) in ref_pairs

def count_keyword_hits(message: str, flag_words: Set[str]) -> Tuple[int, str]:
    """Count keyword hits; returns (count, comma_separated_keywords_hit)."""
    if not message or not flag_words:
        return 0, ""
    toks = set(_tokenize_lower(message))
    hits = sorted(list(toks.intersection(flag_words)))
    return (len(hits), ", ".join(hits))

def process_message(
    payload: Mapping[str, Any],
    ref_pairs: Set[Tuple[str, str]],
    flag_words: Set[str],
) -> Dict[str, Any]:
    """Transform one incoming JSON message into a stored row structure."""
    author = str(payload.get("author", "")).strip()
    message = str(payload.get("message", "")).strip()
    ts = str(payload.get("timestamp", ""))

    valid = validate_author_message(author, message, ref_pairs)
    kw_count, kw_list = count_keyword_hits(message, flag_words)

    row = {
        "src_timestamp": ts,
        "author": author,
        "message": message,
        "author_message_valid": int(valid),
        "keyword_hit_count": int(kw_count),
        "keywords_hit": kw_list,
    }
    logger.info(f"[consumer_data_git_hub] Processed row: {row}")
    return row

def explain_alert_decision(row: Mapping[str, Any]) -> str:
    reasons = []
    kw = int(row.get("keyword_hit_count", 0))
    if kw >= ALERT_KEYWORD_MIN:
        reasons.append(f"keyword_hit_count({kw}) >= ALERT_KEYWORD_MIN({ALERT_KEYWORD_MIN})")
    else:
        reasons.append(f"keyword_hit_count({kw}) < ALERT_KEYWORD_MIN({ALERT_KEYWORD_MIN})")

    if ALERT_ON_VALID_AUTHOR_ONLY:
        if row.get("author_message_valid"):
            reasons.append("ALERT_ON_VALID_AUTHOR_ONLY=True and author_message_valid=1")
        else:
            reasons.append("ALERT_ON_VALID_AUTHOR_ONLY=True but author_message_valid=0")
    else:
        if str(row.get("author", "")).strip():
            reasons.append("ALERT_ON_VALID_AUTHOR_ONLY=False and author is non-empty")
        else:
            reasons.append("ALERT_ON_VALID_AUTHOR_ONLY=False but author is empty")

    return " | ".join(reasons)

# ------------------------------------------------------------------------------------------
# Main (Kafka mode) with resilient poll loop for Windows
# ------------------------------------------------------------------------------------------

def main() -> None:
    logger.info("[consumer_data_git_hub] Starting (Kafka mode).")

    # Read config
    topic = config.get_kafka_topic()
    group_id = config.get_kafka_consumer_group_id()

    # Resolve SQLite path: replace literal "PROJECT_ROOT" segment and absolutize
    raw_sqlite_path = str(config.get_sqlite_path())
    sqlite_str = raw_sqlite_path.replace("PROJECT_ROOT", str(PROJECT_ROOT))
    sqlite_path = pathlib.Path(sqlite_str).resolve()

    logger.info(f"[consumer_data_git_hub] Using SQLite at: {sqlite_path}")

    # Prepare DB
    init_db(sqlite_path)

    # Load reference & flag words
    ref_pairs, _ref_tokens = load_reference_pairs_and_tokens()
    flag_words = load_flag_words()

    # Log policy
    logger.info(
        "[consumer_data_git_hub] ALERT_ON_VALID_AUTHOR_ONLY=%s ALERT_KEYWORD_MIN=%s",
        ALERT_ON_VALID_AUTHOR_ONLY,
        ALERT_KEYWORD_MIN,
    )

    # Soft-check Kafka
    try:
        verify_services(strict=False)
    except Exception as e:
        logger.warning(f"[consumer_data_git_hub] Kafka verify warning: {e}")

    def make_consumer() -> KafkaConsumer:
        return create_kafka_consumer(
            topic_provided=topic,
            group_id_provided=group_id,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )

    consumer = make_consumer()
logger.info("[consumer_data_git_hub] Polling for messages...")
try:
    for msg in consumer:
        payload = msg.value  # already deserialized dict
        row = process_message(payload, ref_pairs, flag_words)

        # Store every processed message
        insert_row(sqlite_path, row)

        # >>> ADD THIS BLOCK HERE <<<
        decision = should_alert(row)
        logger.info("[consumer_data_git_hub] Alert decision=%s :: %s",
                    decision, explain_alert_decision(row))

        if decision:
            insert_alert(sqlite_path, row)
            logger.warning(
                "[ALERT] ts=%s author=%s hits=%s [%s] :: %s",
                row["src_timestamp"],
                row["author"],
                row["keyword_hit_count"],
                row["keywords_hit"] or "none",
                (row["message"][:140] + "…") if len(row["message"]) > 150 else row["message"],
            )

            except ValueError as ve:
                # Windows + kafka-python sometimes throws:
                # ValueError: Invalid file descriptor: -1
                msg = str(ve)
                if "Invalid file descriptor" in msg:
                    logger.error("[consumer_data_git_hub] Poll loop hit '%s'. Recreating consumer...", msg)
                    try:
                        consumer.close()
                    except Exception:
                        pass
                    consumer = make_consumer()
                    continue
                else:
                    raise

            except OSError as oe:
                logger.error("[consumer_data_git_hub] Socket error during poll: %s. Recreating consumer...", oe)
                try:
                    consumer.close()
                except Exception:
                    pass
                consumer = make_consumer()
                continue

            except KeyboardInterrupt:
                logger.warning("[consumer_data_git_hub] Interrupted by user.")
                break

    except Exception as e:
        logger.error(f"[consumer_data_git_hub] Runtime error: {e}")
        raise
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        logger.info("[consumer_data_git_hub] Shutdown complete.")


if __name__ == "__main__":
    main()
