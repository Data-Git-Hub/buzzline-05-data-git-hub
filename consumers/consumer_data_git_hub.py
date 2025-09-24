# consumers/consumer_data_git_hub.py
"""
Custom consumer (Kafka) that:
- Validates (author, message) pairs against a reference corpus (Much Ado).
- Flags messages that contain configured "flag words" from files/flag_words.txt.
- Checks authors against files/bad_authors.txt and tracks counts in SQLite.
- Writes one processed row per message to SQLite (author_validation table).
- Emits "alerts" to console and to SQLite (alerts table) when policy matches.

Run:
  # terminal 1 (producer that emits Much Ado lines)
  py -m producers.producer_much_ado

  # terminal 2 (this consumer)
  py -m consumers.consumer_data_git_hub

Environment overrides (optional):
  ALERT_ON_VALID_AUTHOR_ONLY   -> "1"/"true"/"yes" to require valid author-message pair (default: false)
  ALERT_KEYWORD_MIN            -> integer minimum keyword hits to trigger alert (default: 1)
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

# ======================================================================================
# Env helpers & Alert policy
# ======================================================================================

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
    except ValueError:
        return default

# True  = require that (author,message) exists in reference file AND keyword(s) hit
# False = require non-empty author AND keyword(s) hit
ALERT_ON_VALID_AUTHOR_ONLY: bool = _env_bool("ALERT_ON_VALID_AUTHOR_ONLY", False)
ALERT_KEYWORD_MIN: int = _env_int("ALERT_KEYWORD_MIN", 1)

# ======================================================================================
# Paths (supports both PROJECT_ROOT relative and your absolute layout)
# ======================================================================================

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]

REF_FILE_CANDIDATES: List[pathlib.Path] = [
    PROJECT_ROOT / "files" / "much_ado_excerpt.json",
    PROJECT_ROOT / "files" / "much_ado_except.json",
    PROJECT_ROOT / "files" / "much_ado_execpt.json",
    pathlib.Path(r"C:\Projects\buzzline-05-data-git-hub\files\much_ado_excerpt.json"),
]

FLAG_FILE_CANDIDATES: List[pathlib.Path] = [
    PROJECT_ROOT / "files" / "flag_words.txt",
    PROJECT_ROOT / "files" / "flag_words.json",
    pathlib.Path(r"C:\Projects\buzzline-05-data-git-hub\files\flag_words.txt"),
    pathlib.Path(r"C:\Projects\buzzline-05-data-git-hub\files\flag_words.json"),
]

BAD_AUTHORS_FILE_CANDIDATES: List[pathlib.Path] = [
    PROJECT_ROOT / "files" / "bad_authors.txt",
    pathlib.Path(r"C:\Projects\buzzline-05-data-git-hub\files\bad_authors.txt"),
]

# ======================================================================================
# SQLite schema (+ tiny migration for is_bad_author)
# ======================================================================================

AUTHOR_VALIDATION_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS author_validation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    src_timestamp TEXT,
    author TEXT,
    message TEXT,
    author_message_valid INTEGER,
    keyword_hit_count INTEGER,
    keywords_hit TEXT,
    is_bad_author INTEGER DEFAULT 0
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

BAD_AUTHOR_STATS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS bad_author_stats (
    author TEXT PRIMARY KEY,
    hit_count INTEGER NOT NULL DEFAULT 0
);
"""

def _table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table});")
    cols = {row[1] for row in cur.fetchall()}
    return column in cols

def _maybe_add_is_bad_author(conn: sqlite3.Connection) -> None:
    if not _table_has_column(conn, "author_validation", "is_bad_author"):
        conn.execute("ALTER TABLE author_validation ADD COLUMN is_bad_author INTEGER DEFAULT 0;")

def init_db(db_path: pathlib.Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(AUTHOR_VALIDATION_TABLE_SQL)
        conn.execute(ALERTS_TABLE_SQL)
        conn.execute(BAD_AUTHOR_STATS_TABLE_SQL)
        # migration safeguard if table existed from older runs
        _maybe_add_is_bad_author(conn)
        conn.commit()
    logger.info(f"[consumer_data_git_hub] SQLite ready at {db_path}")

def insert_row(db_path: pathlib.Path, row: Mapping[str, Any]) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO author_validation (
                src_timestamp, author, message, author_message_valid,
                keyword_hit_count, keywords_hit, is_bad_author
            ) VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            (
                row.get("src_timestamp"),
                row.get("author"),
                row.get("message"),
                int(row.get("author_message_valid", 0)),
                int(row.get("keyword_hit_count", 0)),
                row.get("keywords_hit", "") or "",
                int(row.get("is_bad_author", 0)),
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
                row.get("keywords_hit", "") or "",
            ),
        )
        conn.commit()

def increment_bad_author(db_path: pathlib.Path, author: str) -> None:
    if not author:
        return
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO bad_author_stats(author, hit_count) VALUES(?, 1)
            ON CONFLICT(author) DO UPDATE SET hit_count = hit_count + 1;
            """,
            (author.strip().lower(),),
        )
        conn.commit()

# ======================================================================================
# Reference loader, tokens, flag words, bad authors
# ======================================================================================

_TOKEN_RE = re.compile(r"[A-Za-z']+")

def _tokenize_lower(text: str) -> List[str]:
    return [t.lower() for t in _TOKEN_RE.findall(text or "")]

def _first_existing(candidates: Sequence[pathlib.Path]) -> Optional[pathlib.Path]:
    for p in candidates:
        if p.exists():
            return p
    return None

def load_reference_pairs_and_tokens() -> Tuple[Set[Tuple[str, str]], List[str]]:
    ref_path = _first_existing(REF_FILE_CANDIDATES)
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

def load_flag_words() -> Set[str]:
    p = _first_existing(FLAG_FILE_CANDIDATES)
    if not p:
        logger.warning("[consumer_data_git_hub] No flag words file found; keyword alerts will never trigger.")
        return set()
    try:
        if p.suffix.lower() == ".json":
            words = set(json.loads(p.read_text(encoding="utf-8")))
        else:
            words = {
                w.strip().lower()
                for w in p.read_text(encoding="utf-8").splitlines()
                if w.strip() and not w.strip().startswith("#")
            }
        logger.info(f"[consumer_data_git_hub] Loaded {len(words)} flag words from {p}")
        return words
    except Exception as e:
        logger.error(f"[consumer_data_git_hub] Failed to load flag words from {p}: {e}")
        return set()

def load_bad_authors() -> Set[str]:
    p = _first_existing(BAD_AUTHORS_FILE_CANDIDATES)
    if not p:
        logger.warning("[consumer_data_git_hub] No bad_authors.txt found; bad author tracking disabled.")
        return set()
    try:
        authors = {
            a.strip().lower()
            for a in p.read_text(encoding="utf-8").splitlines()
            if a.strip() and not a.strip().startswith("#")
        }
        logger.info(f"[consumer_data_git_hub] Loaded {len(authors)} bad authors from {p}")
        return authors
    except Exception as e:
        logger.error(f"[consumer_data_git_hub] Failed to load bad authors from {p}: {e}")
        return set()

# ======================================================================================
# Per-message processing & alert logic
# ======================================================================================

def validate_author_message(
    author: str,
    message: str,
    ref_pairs: Set[Tuple[str, str]],
) -> bool:
    a = (author or "").strip().lower()
    m = (message or "").strip().lower()
    return (a, m) in ref_pairs

def count_keyword_hits(message: str, flag_words: Set[str]) -> Tuple[int, str]:
    if not message or not flag_words:
        return 0, ""
    toks = set(_tokenize_lower(message))
    hits = sorted(list(toks.intersection(flag_words)))
    return (len(hits), ", ".join(hits))

def should_alert(row: Mapping[str, Any]) -> bool:
    kw = int(row.get("keyword_hit_count", 0))
    if kw < ALERT_KEYWORD_MIN:
        return False

    if ALERT_ON_VALID_AUTHOR_ONLY:
        return bool(row.get("author_message_valid"))
    else:
        return bool(str(row.get("author", "")).strip())

def explain_alert_decision(row: Mapping[str, Any]) -> str:
    parts = []
    kw = int(row.get("keyword_hit_count", 0))
    parts.append(
        f"keyword_hit_count({kw}) {'>=' if kw >= ALERT_KEYWORD_MIN else '<'} ALERT_KEYWORD_MIN({ALERT_KEYWORD_MIN})"
    )
    if ALERT_ON_VALID_AUTHOR_ONLY:
        parts.append(f"ALERT_ON_VALID_AUTHOR_ONLY=True; author_message_valid={row.get('author_message_valid')}")
    else:
        parts.append(
            f"ALERT_ON_VALID_AUTHOR_ONLY=False; author_non_empty={bool(str(row.get('author','')).strip())}"
        )
    return " | ".join(parts)

def process_message(
    payload: Mapping[str, Any],
    ref_pairs: Set[Tuple[str, str]],
    flag_words: Set[str],
    bad_authors: Set[str],
) -> Dict[str, Any]:
    author = str(payload.get("author", "")).strip()
    message = str(payload.get("message", "")).strip()
    ts = str(payload.get("timestamp", ""))

    valid = validate_author_message(author, message, ref_pairs)
    kw_count, kw_list = count_keyword_hits(message, flag_words)
    is_bad_author = int(author.lower() in bad_authors) if author else 0

    row = {
        "src_timestamp": ts,
        "author": author,
        "message": message,
        "author_message_valid": int(valid),
        "keyword_hit_count": int(kw_count),
        "keywords_hit": kw_list,
        "is_bad_author": is_bad_author,
    }
    logger.info(f"[consumer_data_git_hub] Processed row: {row}")
    return row

# ======================================================================================
# Main
# ======================================================================================

def main() -> None:
    logger.info("[consumer_data_git_hub] Starting (Kafka mode).")

    # Read config
    topic = config.get_kafka_topic()
    group_id = config.get_kafka_consumer_group_id()
    sqlite_path = config.get_sqlite_path()

    # Prepare DB (incl. migration)
    init_db(sqlite_path)

    # Load reference, flags, bad authors
    ref_pairs, _ = load_reference_pairs_and_tokens()
    flag_words = load_flag_words()
    bad_authors = load_bad_authors()

    # Log active alert policy
    logger.info(
        f"[consumer_data_git_hub] ALERT_ON_VALID_AUTHOR_ONLY={ALERT_ON_VALID_AUTHOR_ONLY} "
        f"ALERT_KEYWORD_MIN={ALERT_KEYWORD_MIN}"
    )

    # Verify Kafka up (soft)
    try:
        verify_services(strict=False)
    except Exception as e:
        logger.warning(f"[consumer_data_git_hub] Kafka verify warning: {e}")

    # Create Kafka consumer
    consumer: KafkaConsumer = create_kafka_consumer(
        topic_provided=topic,
        group_id_provided=group_id,
        value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
    )

    logger.info("[consumer_data_git_hub] Polling for messages...")
    try:
        for msg in consumer:
            payload = msg.value  # already deserialized dict
            row = process_message(payload, ref_pairs, flag_words, bad_authors)

            # Store every processed message
            insert_row(sqlite_path, row)

            # Track bad author counts
            if row.get("is_bad_author"):
                increment_bad_author(sqlite_path, row.get("author", ""))

            # Alert decision & record
            decision = should_alert(row)
            logger.info(
                "[consumer_data_git_hub] Alert decision=%s :: %s",
                decision,
                explain_alert_decision(row),
            )
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

    except KeyboardInterrupt:
        logger.warning("[consumer_data_git_hub] Interrupted by user.")
    except Exception as e:
        logger.error(f"[consumer_data_git_hub] Runtime error: {e}")
        raise
    finally:
        logger.info("[consumer_data_git_hub] Shutdown complete.")

# ======================================================================================
# Entry
# ======================================================================================

if __name__ == "__main__":
    main()
