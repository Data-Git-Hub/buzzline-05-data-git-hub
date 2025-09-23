"""
tests/test_emitters.py

Lightweight tests to verify emitters append / publish without side-effects.
Uses temp paths and skips Kafka when broker isn't available.

Requires pytest (add to requirements.txt).

Usage:
  pytest -q
"""

#####################################
# Imports
#####################################

import json
import pathlib
import socket
import sqlite3
import pytest

from utils.emitters import file_emitter, sqlite_emitter, duckdb_emitter

#####################################
# Helper Functions
#####################################


def _broker_up(host: str="localhost", port: int=9092, timeout:float=0.2) -> bool:
    """Return True if a TCP connection to Kafka succeeds."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


#####################################
# File Emitter
#####################################


def test_file_emitter_appends_jsonl(tmp_path: pathlib.Path):
    out = tmp_path / "live.jsonl"
    msg1 = {"message": "hello", "author": "Alice"}
    msg2 = {"message": "world", "author": "Bob"}

    assert file_emitter.emit_message(msg1, path=out) is True
    assert file_emitter.emit_message(msg2, path=out) is True

    content = out.read_text(encoding="utf-8").strip().splitlines()
    assert len(content) == 2
    assert json.loads(content[0])["author"] == "Alice"
    assert json.loads(content[1])["message"] == "world"


#####################################
# SQLite Emitter
#####################################


def test_sqlite_emitter_inserts_rows(tmp_path: pathlib.Path):
    db_path = tmp_path / "test.sqlite"
    msg = {
        "message": "m1",
        "author": "A",
        "timestamp": "2025-01-01 00:00:00",
        "category": "x",
        "sentiment": 0.5,
        "keyword_mentioned": "k",
        "message_length": 2,
    }

    assert sqlite_emitter.emit_message(msg, db_path=db_path) is True

    # Verify row count directly
    with sqlite3.connect(str(db_path)) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM streamed_messages;")
        (count,) = cur.fetchone()
        assert count == 1


#####################################
# DuckDB Emitter
#####################################


@pytest.mark.optional
def test_duckdb_emitter_inserts_rows(tmp_path: pathlib.Path):
    try:
        from utils.emitters import duckdb_emitter  # keep duckdb optional
        import duckdb  # type: ignore
    except Exception as e:
        pytest.skip(f"DuckDB not installed/available: {e}")

    db_path = tmp_path / "test.duckdb"
    msg = {
        "message": "m2",
        "author": "B",
        "timestamp": "2025-01-01 00:00:01",
        "category": "y",
        "sentiment": 0.7,
        "keyword_mentioned": "k2",
        "message_length": 3,
    }

    assert duckdb_emitter.emit_message(msg, db_path=db_path) is True

    # Verify row count directly
    con = duckdb.connect(database=str(db_path), read_only=True)
    try:
        (count,) = con.execute("SELECT COUNT(*) FROM streamed_messages;").fetchone()
        assert count == 1
    finally:
        con.close()


#####################################
# Kafka Emitter (optional)
#####################################


@pytest.mark.skipif(not _broker_up(), reason="Kafka broker not reachable on localhost:9092")
def test_kafka_emitter_publishes():
    # Lazy import to avoid hard dependency when broker is down
    from utils.emitters import kafka_emitter
    from kafka import KafkaProducer  # kafka-python-ng

    topic = "test_topic_pytest"
    producer = KafkaProducer(bootstrap_servers="localhost:9092")

    try:
        assert kafka_emitter.emit_message(
            {"message": "pytest", "author": "T"}, producer=producer, topic=topic
        ) is True
    finally:
        try:
            producer.close()
        except Exception:
            pass
