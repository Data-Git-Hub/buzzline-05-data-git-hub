# consumers/consumer_data_git_hub.py
"""
Custom consumer (Kafka) that:
- Validates (author, message) pairs against a reference corpus (Much Ado).
- Flags messages that contain configured "flag words" (from files/flag_words.txt).
- Flags messages from "bad authors" (from files/bad_authors.txt).
- Writes one processed row per message to SQLite (author_validation table).
- Emits "alerts" to console and to SQLite (alerts table) when policy matches.

Run:
  # terminal 1 - your Much Ado producer
  py -m producers.producer_much_ado

  # terminal 2 - this consumer
  py -m consumers.consumer_data_git_hub

Env overrides (optional):
  set ALERT_ON_VALID_AUTHOR_ONLY=true|false
  set ALERT_KEYWORD_MIN=0|1|2...
"""

from __future__ import annotations

# stdlib
import json
import os
import re
import sqlite3
import pathlib
from collections import Counter
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set, Tuple

# external
from kafka import KafkaConsumer

# local
import utils.utils_config as config
from utils.utils_logger import logger
from utils.utils_consumer import create_kafka_consumer
from utils.utils_producer import verify_services


# --------------------------------------------------------------------------------------
# Environment + constants
# --------------------------------------------------------------------------------------

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

# Alert policy env
ALERT_ON_VALID_AUTHOR_ONLY: bool = _env_bool("ALERT_ON_VALID_AUTHOR_ONLY", False)
ALERT_KEYWORD_MIN: int = _env_int("ALERT_KEYWORD_MIN", 1)

# Tokenizer
_TOKEN_RE = re.compile(r"[A-Za-z']+")


# --------------------------------------------------------------------------------------
# Paths (derive PROJECT_ROOT from config's BASE_DATA_PATH)
# --------------------------------------------------------------------------------------

def _project_root_from_config() -> pathlib.Path:
    """
    Your repo exposes utils_config.get_base_data_path() (string).
    It typically returns something like "PROJECT_ROOT/data".
    We convert to Path and step up one level if it ends with "data".
    """
    try:
        base_str: str = config.get_base_data_path()
    except Exception:
        # Fallback if API differs or missing
        base_str = "data"
    base = pathlib.Path(base_str)
    if base.name.lower() == "data":
        return base.parent if base.parent != pathlib.Path("") else pathlib.Path(".").resolve()
    # If it already points at project root, use it as-is
    return base.resolve()

PROJECT_ROOT = _project_root_from_config()
FILES_DIR = PROJECT_ROOT / "files"

REF_FILE = FILES_DIR / "much_ado_excerpt.json"     # reference corpus
FLAG_WORDS_FILE = FILES_DIR / "flag_words.txt"     # one word per line
BAD_AUTHORS_FILE = FILES_DIR / "bad_authors.txt"   # one author per line (lowercase recommended)


# --------------------------------------------------------------------------------------
# SQLite schema + migration
# --------------------------------------------------------------------------------------

AUTHOR_VALIDATION_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS author_validation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    src_timestamp TEXT,
    author TEXT,
    message TEXT,
    author_message_valid INTEGER,
    keyword_hit_count INTEGER,
    keywords_hit TEXT,
    bad_author_flag INTEGER DEFAULT 0
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
    keywords_hit TEXT,
    bad_author_flag INTEGER DEFAULT 0
);
"""

def _table_has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table});")
    cols = [r[1].lower() for r in cur.fetchall()]  # (cid,name,type,notnull,dflt,pk)
    return col.lower() in cols

def migrate_author_validation(conn: sqlite3.Connection) -> None:
    conn.execute(AUTHOR_VALIDATION_TABLE_SQL)
    if not _table_has_column(conn, "author_validation", "bad_author_flag"):
        conn.execute("ALTER TABLE author_validation ADD COLUMN bad_author_flag INTEGER DEFAULT 0;")

def migrate_alerts(conn: sqlite3.Connection) -> None:
    conn.execute(ALERTS_TABLE_SQL)
    if not _table_has_column(conn, "alerts", "bad_author_flag"):
        conn.execute("ALTER TABLE alerts ADD COLUMN bad_author_flag INTEGER DEFAULT 0;")

def init_db(db_path: pathlib.Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path)) as conn:
        migrate_author_validation(conn)
        migrate_alerts(conn)
        conn.commit()
    logger.info(f"[consumer_data_git_hub] SQLite ready at {db_path}")


def insert_row(db_path: pathlib.Path, row: Mapping[str, Any]) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO author_validation (
                src_timestamp, author, message, author_message_valid,
                keyword_hit_count, keywords_hit, bad_author_flag
            ) VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            (
                row.get("src_timestamp"),
                row.get("author"),
                row.get("message"),
                int(row.get("author_message_valid", 0)),
                int(row.get("keyword_hit_count", 0)),
                row.get("keywords_hit", ""),
                int(row.get("bad_author_flag", 0)),
            ),
        )
        conn.commit()
    logger.info("[consumer_data_git_hub] Inserted one processed row.")

def insert_alert(db_path: pathlib.Path, row: Mapping[str, Any]) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO alerts (
                src_timestamp, author, message,
                keyword_hit_count, keywords_hit, bad_author_flag
            ) VALUES (?, ?, ?, ?, ?, ?);
            """,
            (
                row.get("src_timestamp"),
                row.get("author"),
                row.get("message"),
                int(row.get("keyword_hit_count", 0)),
                row.get("keywords_hit", ""),
                int(row.get("bad_author_flag", 0)),
            ),
        )
        conn.commit()


# --------------------------------------------------------------------------------------
# Loading helpers (reference / flags / bad authors)
# --------------------------------------------------------------------------------------

def _tokenize_lower(text: str) -> List[str]:
    return [t.lower() for t in _TOKEN_RE.findall(text or "")]

def load_reference_pairs_and_tokens() -> Tuple[Set[Tuple[str, str]], List[str]]:
    """
    Load the Much Ado reference JSON. Returns:
      - a set of (author_lower, message_stripped_lower)
      - a flat list of tokens from all messages (lowercased)
    """
    if not REF_FILE.exists():
        logger.warning(f"[consumer_data_git_hub] No reference file found at {REF_FILE}; author validation will be 0.")
        return set(), []

    try:
        data = json.loads(REF_FILE.read_text(encoding="utf-8"))
        pairs: Set[Tuple[str, str]] = set()
        tokens: List[str] = []
        for item in data:
            author = str(item.get("author", "")).strip().lower()
            message = str(item.get("message", "")).strip().lower()
            if author and message:
                pairs.add((author, message))
            tokens.extend(_tokenize_lower(message))
        logger.info(f"[consumer_data_git_hub] Loaded reference pairs: {len(pairs)} from {REF_FILE}")
        return pairs, tokens
    except Exception as e:
        logger.error(f"[consumer_data_git_hub] Failed to read reference file: {e}")
        return set(), []

def _top_tokens_from_messages(
    tokens: Iterable[str],
    stopset: Set[str],
    top_n: int = 10,
    min_len: int = 3,
) -> List[str]:
    filtered = [t for t in tokens if len(t) >= min_len and t not in stopset]
    counts = Counter(filtered)
    return [w for w, _ in counts.most_common(top_n)]

# Tiny stop set used only when deriving defaults (no external deps)
_DERIVE_STOP = {
    "the","and","to","of","a","i","in","you","that","it","is","my","for","with","not","be","me",
    "he","his","she","her","we","they","them","as","but","have","had","are","on","so","do","this",
    "your","at","by","or","from","what","who","there","no","will","if","did","shall","their","an",
    "were","which","when","all","our","now","than","then","been","into","would","should","could",
    "was","o","oh","ye","thou","thee","thy","sir","lord","lady"
}

def load_flag_words() -> Set[str]:
    """
    Load flag words from FILES_DIR/flag_words.txt (one per line).
    If missing, derive ~10 common tokens from reference (minus small stop set) and write the file.
    """
    if FLAG_WORDS_FILE.exists():
        try:
            words = {
                w.strip().lower()
                for w in FLAG_WORDS_FILE.read_text(encoding="utf-8").splitlines()
                if w.strip()
            }
            logger.info(f"[consumer_data_git_hub] Loaded {len(words)} flag words from {FLAG_WORDS_FILE}")
            return words
        except Exception as e:
            logger.warning(f"[consumer_data_git_hub] Could not read flag words: {e}")

    # Derive if missing
    pairs, tokens = load_reference_pairs_and_tokens()
    if tokens:
        derived = set(_top_tokens_from_messages(tokens, _DERIVE_STOP, top_n=10, min_len=3))
        try:
            FLAG_WORDS_FILE.parent.mkdir(parents=True, exist_ok=True)
            FLAG_WORDS_FILE.write_text("\n".join(sorted(derived)) + "\n", encoding="utf-8")
            logger.info(f"[consumer_data_git_hub] Derived {len(derived)} flag words and wrote {FLAG_WORDS_FILE}")
        except Exception as e:
            logger.warning(f"[consumer_data_git_hub] Could not write derived flag words: {e}")
        return derived

    logger.warning("[consumer_data_git_hub] No flag words (no file and no reference).")
    return set()

def load_bad_authors() -> Set[str]:
    """Load bad authors from FILES_DIR/bad_authors.txt (one per line, case-insensitive)."""
    if not BAD_AUTHORS_FILE.exists():
        logger.info(f"[consumer_data_git_hub] No bad_authors.txt found at {BAD_AUTHORS_FILE}.")
        return set()
    try:
        bad = {
            a.strip().lower()
            for a in BAD_AUTHORS_FILE.read_text(encoding="utf-8").splitlines()
            if a.strip()
        }
        logger.info(f"[consumer_data_git_hub] Loaded {len(bad)} bad authors from {BAD_AUTHORS_FILE}")
        return bad
    except Exception as e:
        logger.warning(f"[consumer_data_git_hub] Could not read bad_authors.txt: {e}")
        return set()


# --------------------------------------------------------------------------------------
# Per-message processing + alerts
# --------------------------------------------------------------------------------------

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
        return 0, "none"
    toks = set(_tokenize_lower(message))
    hits = sorted(list(toks.intersection(flag_words)))
    return (len(hits), ", ".join(hits) if hits else "none")

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
    bad_author_flag = 1 if author.lower() in bad_authors else 0

    row = {
        "src_timestamp": ts,
        "author": author,
        "message": message,
        "author_message_valid": int(valid),
        "keyword_hit_count": int(kw_count),
        "keywords_hit": kw_list,
        "bad_author_flag": bad_author_flag,
    }
    logger.info(f"[consumer_data_git_hub] Processed row: {row}")
    return row

def explain_alert_decision(row: Mapping[str, Any]) -> str:
    parts = []
    parts.append("bad_author=YES" if int(row.get("bad_author_flag", 0)) == 1 else "bad_author=NO")
    parts.append(f"kw_hits={row.get('keyword_hit_count', 0)} (min={ALERT_KEYWORD_MIN})")
    parts.append(f"author_valid={bool(row.get('author_message_valid'))}")
    parts.append(f"policy_valid_only={ALERT_ON_VALID_AUTHOR_ONLY}")
    return " | ".join(parts)

def should_alert(row: Mapping[str, Any]) -> bool:
    # Always alert on bad author
    if int(row.get("bad_author_flag", 0)) == 1:
        return True
    # Require at least N keyword hits
    if int(row.get("keyword_hit_count", 0)) < ALERT_KEYWORD_MIN:
        return False
    # If configured, only alert when (author,message) is known good pair
    if ALERT_ON_VALID_AUTHOR_ONLY:
        return bool(row.get("author_message_valid"))
    # Otherwise, keywords are sufficient
    return True


# --------------------------------------------------------------------------------------
# Main (Kafka)
# --------------------------------------------------------------------------------------

def main() -> None:
    logger.info("[consumer_data_git_hub] Starting (Kafka mode).")

    # Config / paths
    sqlite_path = config.get_sqlite_path()
    logger.info(f"[consumer_data_git_hub] Using SQLite at: {sqlite_path}")

    # Prepare DB (no deletion; append-only)
    init_db(sqlite_path)

    # Load resources
    ref_pairs, _ref_tokens = load_reference_pairs_and_tokens()
    flag_words = load_flag_words()
    bad_authors = load_bad_authors()

    logger.info(
        f"[consumer_data_git_hub] ALERT_ON_VALID_AUTHOR_ONLY={ALERT_ON_VALID_AUTHOR_ONLY} "
        f"ALERT_KEYWORD_MIN={ALERT_KEYWORD_MIN}"
    )

    # Verify Kafka (soft)
    try:
        verify_services(strict=False)
    except Exception as e:
        logger.warning(f"[consumer_data_git_hub] Kafka verify warning: {e}")

    # Build consumer
    topic = config.get_kafka_topic()
    group_id = config.get_kafka_consumer_group_id()
    logger.info(f"BUZZ_TOPIC: {topic}")
    logger.info(f"BUZZ_CONSUMER_GROUP_ID: {group_id}")

    consumer: KafkaConsumer = create_kafka_consumer(
        topic_provided=topic,
        group_id_provided=group_id,
        value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
    )

    logger.info("[consumer_data_git_hub] Polling for messages...")
    try:
        for msg in consumer:
            payload = msg.value  # dict from producer_much_ado
            row = process_message(payload, ref_pairs, flag_words, bad_authors)

            # Store every processed message
            insert_row(sqlite_path, row)

            # Decide + store alert if needed
            decision = should_alert(row)
            logger.info(f"[consumer_data_git_hub] Alert decision={decision} :: {explain_alert_decision(row)}")

            if decision:
                insert_alert(sqlite_path, row)
                preview = (row["message"][:140] + "…") if len(row["message"]) > 150 else row["message"]
                logger.warning(
                    f"[ALERT] ts={row['src_timestamp']} author={row['author']} "
                    f"hits={row['keyword_hit_count']} [{row['keywords_hit']}] :: {preview}"
                )

    except KeyboardInterrupt:
        logger.warning("[consumer_data_git_hub] Interrupted by user.")
    except Exception as e:
        logger.error(f"[consumer_data_git_hub] Runtime error: {e}")
        raise
    finally:
        logger.info("[consumer_data_git_hub] Shutdown complete.")


if __name__ == "__main__":
    main()
