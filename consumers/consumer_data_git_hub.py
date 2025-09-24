# consumers/consumer_data_git_hub.py
"""
Custom consumer (Kafka) that:
- Validates (author, message) pairs against a reference corpus (Much Ado).
- Flags messages that contain configured "flag words".
- Writes one processed row per message to SQLite (author_validation table).
- Emits "alerts" to console and to SQLite (alerts table) when policy matches.

Run:
  # terminal 1
  py -m producers.producer_case

  # terminal 2
  py -m consumers.consumer_data_git_hub
"""

from __future__ import annotations

# stdlib
import json
import os
import re
import sqlite3
import string
import pathlib
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

# external
from kafka import KafkaConsumer

# local
import utils.utils_config as config
from utils.utils_logger import logger
from utils.utils_consumer import create_kafka_consumer
from utils.utils_producer import verify_services

# --------------------------------------------------------------------------------------------------
# Alert policy
# --------------------------------------------------------------------------------------------------
# True  = require that (author,message) exists in reference file AND keyword(s) hit
# False = require non-empty author AND keyword(s) hit
ALERT_ON_VALID_AUTHOR_ONLY: bool = True

# --------------------------------------------------------------------------------------------------
# Reference & flags paths
# --------------------------------------------------------------------------------------------------

REF_FILE_CANDIDATES: List[pathlib.Path] = [
    pathlib.Path(r"C:\Projects\files\buzzline-05-data-git-hub\much_ado_excerpt.json"),  # preferred
    pathlib.Path(r"C:\Projects\files\buzzline-05-data-git-hub\much_ado_except.json"),
    pathlib.Path(r"C:\Projects\files\buzzline-05-data-git-hub\much_ado_execpt.json"),
]

FLAG_FILE_CANDIDATES: List[pathlib.Path] = [
    pathlib.Path(r"C:\Projects\files\buzzline-05-data-git-hub\flag_words"),
    pathlib.Path(r"C:\Projects\files\buzzline-05-data-git-hub\flag_words.txt"),
    pathlib.Path(r"C:\Projects\files\buzzline-05-data-git-hub\flag_words.json"),
]

# If no flag file found, we will generate this file from the reference JSON:
DEFAULT_FLAG_WORDS_OUT: pathlib.Path = pathlib.Path(
    r"C:\Projects\files\buzzline-05-data-git-hub\flag_words.txt"
)

# --------------------------------------------------------------------------------------------------
# SQLite schema
# --------------------------------------------------------------------------------------------------

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

# --------------------------------------------------------------------------------------------------
# DB helpers
# --------------------------------------------------------------------------------------------------


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
    if int(row.get("keyword_hit_count", 0)) <= 0:
        return False

    if ALERT_ON_VALID_AUTHOR_ONLY:
        return bool(row.get("author_message_valid"))
    else:
        return bool(str(row.get("author", "")).strip())


# --------------------------------------------------------------------------------------------------
# Reference loader (Much Ado)
# --------------------------------------------------------------------------------------------------


def load_reference_pairs() -> Set[Tuple[str, str]]:
    """Return a set of (author_lower, exact_message) from first existing ref file."""
    for p in REF_FILE_CANDIDATES:
        try:
            if p.exists():
                data = json.loads(p.read_text(encoding="utf-8"))
                pairs: Set[Tuple[str, str]] = set()
                for item in data:
                    # tolerant parsing
                    msg = str(item.get("message", "")).strip()
                    auth = str(item.get("author", "")).strip().lower()
                    if msg and auth:
                        pairs.add((auth, msg))
                logger.info(f"[consumer_data_git_hub] Using reference file: {p}")
                logger.info(f"[consumer_data_git_hub] Loaded {len(pairs)} ref pairs.")
                return pairs
        except Exception as e:
            logger.warning(f"[consumer_data_git_hub] Failed to load reference {p}: {e}")
    logger.warning(
        "[consumer_data_git_hub] No reference file found; author validation will be 0."
    )
    return set()


# --------------------------------------------------------------------------------------------------
# Flag words: load or auto-generate from reference JSON
# --------------------------------------------------------------------------------------------------

# A small, hand-rolled stopword set (so we don’t add dependencies).
_STOPWORDS = {
    "the",
    "and",
    "to",
    "of",
    "a",
    "i",
    "in",
    "you",
    "that",
    "it",
    "is",
    "my",
    "for",
    "with",
    "not",
    "be",
    "me",
    "he",
    "his",
    "she",
    "her",
    "we",
    "they",
    "them",
    "as",
    "but",
    "have",
    "had",
    "are",
    "on",
    "so",
    "do",
    "this",
    "your",
    "at",
    "by",
    "or",
    "from",
    "what",
    "who",
    "there",
    "no",
    "will",
    "if",
    "did",
    "shall",
    "their",
    "an",
    "were",
    "which",
    "when",
    "all",
    "our",
    "now",
    "than",
    "then",
    "been",
    "into",
    "would",
    "should",
    "could",
    "was",
    "o",
    "oh",
    "ye",
    "thou",
    "thee",
    "thy",
    "thee",
    "sir",
    "lord",
    "lady",
}


_WORD_RE = re.compile(r"[A-Za-z][A-Za-z']+")  # keep simple words with apostrophes


def _tokenize(text: str) -> Iterable[str]:
    for w in _WORD_RE.findall(text.lower()):
        yield w.strip("'")


def _load_raw_messages_from_ref() -> List[str]:
    """Load raw messages from the first existing reference file."""
    for p in REF_FILE_CANDIDATES:
        try:
            if p.exists():
                data = json.loads(p.read_text(encoding="utf-8"))
                msgs = []
                for item in data:
                    msg = str(item.get("message", "")).strip()
                    if msg:
                        msgs.append(msg)
                return msgs
        except Exception:
            pass
    return []


def _top_tokens_from_messages(msgs: Sequence[str], top_n: int = 10) -> List[str]:
    """Find top-N frequent tokens (simple heuristic -> mostly nouns/verbs)."""
    from collections import Counter

    cnt: Counter[str] = Counter()
    for m in msgs:
        for tok in _tokenize(m):
            if tok in _STOPWORDS:
                continue
            if len(tok) < 3:
                continue
            cnt[tok] += 1

    # pick most common; this generally yields nouns/verbs without full POS tagging
    return [w for (w, _) in cnt.most_common(top_n)]


def _save_flag_words_plain(words: Sequence[str], out_path: pathlib.Path) -> None:
    """Save newline-separated file."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(words) + "\n", encoding="utf-8")


def _load_flag_words_file(path: pathlib.Path) -> List[str]:
    """Load words from .json or plain text."""
    try:
        if path.suffix.lower() == ".json":
            data = json.loads(path.read_text(encoding="utf-8"))
            words = [str(w).strip().lower() for w in data if str(w).strip()]
            return words
        else:
            # plain text: newline or comma separated
            raw = path.read_text(encoding="utf-8")
            parts = re.split(r"[\n,]+", raw)
            words = [p.strip().lower() for p in parts if p.strip()]
            return words
    except Exception as e:
        logger.warning(f"[consumer_data_git_hub] Failed to load flags from {path}: {e}")
        return []


def load_or_build_flag_words() -> List[str]:
    # 1) look for an existing flag words file
    for p in FLAG_FILE_CANDIDATES:
        if p.exists():
            words = _load_flag_words_file(p)
            if words:
                logger.info(f"[consumer_data_git_hub] Using flag words file: {p}")
                logger.info(f"[consumer_data_git_hub] Loaded {len(words)} flag words.")
                return words

    # 2) not found -> try to build from reference JSON
    msgs = _load_raw_messages_from_ref()
    if msgs:
        top10 = _top_tokens_from_messages(msgs, top_n=10)
        _save_flag_words_plain(top10, DEFAULT_FLAG_WORDS_OUT)
        logger.info(
            "[consumer_data_git_hub] No flag words file found. "
            f"Built top-10 list from reference and saved to {DEFAULT_FLAG_WORDS_OUT}: {top10}"
        )
        return top10

    # 3) no reference either -> none (alerts won’t trigger)
    logger.warning(
        "[consumer_data_git_hub] No flag words file found; keyword hits will be 0."
    )
    return []


# --------------------------------------------------------------------------------------------------
# Message processing
# --------------------------------------------------------------------------------------------------


def _find_keyword_hits(message_text: str, flag_words: Sequence[str]) -> List[str]:
    """Return the list of flag words found in the message (case-insensitive, token-wise)."""
    if not flag_words:
        return []
    msg_tokens = set(_tokenize(message_text))
    hits = [w for w in flag_words if w in msg_tokens]
    return hits


def process_message(
    msg: Mapping[str, Any], ref_pairs: Set[Tuple[str, str]], flag_words: Sequence[str]
) -> Optional[Dict[str, Any]]:
    try:
        raw_message = str(msg.get("message", "")).strip()
        raw_author = str(msg.get("author", "")).strip()
        ts = str(msg.get("timestamp", "")).strip()

        if not raw_message:
            return None

        author_norm = raw_author.lower()

        author_message_valid = (author_norm, raw_message) in ref_pairs if ref_pairs else False
        hits = _find_keyword_hits(raw_message, flag_words)

        row = {
            "src_timestamp": ts,
            "author": raw_author,
            "message": raw_message,
            "author_message_valid": 1 if author_message_valid else 0,
            "keyword_hit_count": len(hits),
            "keywords_hit": ",".join(hits),
        }

        logger.info(f"[consumer_data_git_hub] Processed row: {row}")
        return row
    except Exception as e:
        logger.error(f"[consumer_data_git_hub] Error processing message: {e}")
        return None


# --------------------------------------------------------------------------------------------------
# Kafka consume loop
# --------------------------------------------------------------------------------------------------


def consume_messages_from_kafka(
    topic: str,
    group_id: str,
    sqlite_path: pathlib.Path,
    ref_pairs: Set[Tuple[str, str]],
    flag_words: Sequence[str],
) -> None:
    # Step 1. Verify Kafka services
    verify_services(strict=True)

    # Step 2. Create consumer
    consumer: KafkaConsumer = create_kafka_consumer(
        topic,
        group_id,
        value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
    )

    logger.info("[consumer_data_git_hub] Polling for messages...")

    # Step 3. Iterate
    try:
        for message in consumer:
            # message.value is already a dict (due to deserializer above)
            row = process_message(message.value, ref_pairs, flag_words)
            if not row:
                continue

            insert_row(sqlite_path, row)

            # Alert path
            if should_alert(row):
                logger.warning(
                    "[ALERT] author=%r keywords=%r msg=%r",
                    row.get("author"),
                    row.get("keywords_hit"),
                    row.get("message"),
                )
                insert_alert(sqlite_path, row)
    except KeyboardInterrupt:
        logger.warning("[consumer_data_git_hub] Interrupted by user.")
    except Exception as e:
        logger.error(f"[consumer_data_git_hub] Unexpected error: {e}")
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        logger.info("[consumer_data_git_hub] Shutdown complete.")


# --------------------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------------------


def main() -> None:
    logger.info("[consumer_data_git_hub] Starting (Kafka mode).")

    # Read config values
    topic = config.get_kafka_topic()
    group_id = config.get_kafka_consumer_group_id()
    sqlite_path = config.get_sqlite_path()

    # Prepare DB
    init_db(sqlite_path)

    # Load reference set & flags
    ref_pairs = load_reference_pairs()
    flag_words = load_or_build_flag_words()

    # Run Kafka consume loop
    consume_messages_from_kafka(topic, group_id, sqlite_path, ref_pairs, flag_words)


# --------------------------------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    main()
