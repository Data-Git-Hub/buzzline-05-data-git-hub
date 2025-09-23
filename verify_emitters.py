"""
verify_emitters.py

Quick manual check that each emitter works.

Usage:
  py -m verify_emitters
"""

import pathlib
import socket
from typing import Any

from utils.emitters import file_emitter, sqlite_emitter


def broker_up(host: str = "localhost", port: int = 9092, timeout: float = 0.2) -> bool:
    """Return True if a TCP connection to the broker succeeds."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def main() -> None:
    """Run quick verification for all emitters."""
    msg: dict[str, Any] = {
        "message": "hello world",
        "author": "Demo",
        "timestamp": "2025-01-01 00:00:00",
        "category": "demo",
        "sentiment": 0.42,
        "keyword_mentioned": "hello",
        "message_length": 11,
    }

    # File
    out = pathlib.Path("data/demo.jsonl")
    file_emitter.emit_message(msg, path=out)
    print(f"[file] appended 1 line to {out}")

    # SQLite
    db_path = pathlib.Path("data/demo.sqlite")
    sqlite_emitter.emit_message(msg, db_path=db_path)
    print(f"[sqlite] wrote 1 row to {db_path} (table: streamed_messages)")

    # DuckDB (optional)
    try:
        from utils.emitters import duckdb_emitter
        duck_path = pathlib.Path("data/demo.duckdb")
        duckdb_emitter.emit_message(msg, db_path=duck_path)
        print(f"[duckdb] wrote 1 row to {duck_path} (table: streamed_messages)")
    except Exception as e:
        print(f"[duckdb] skipped: {e}")

    # Kafka (optional; only if broker is up)
    if broker_up():
        try:
            from utils.emitters import kafka_emitter
            from kafka import KafkaProducer
            producer = KafkaProducer(bootstrap_servers="localhost:9092")
            kafka_emitter.emit_message(msg, producer=producer, topic="demo_topic")
            producer.close()
            print("[kafka] published 1 message to 'demo_topic'")
        except Exception as e:
            print(f"[kafka] failed: {e}")
    else:
        print("[kafka] skipped: broker not reachable on localhost:9092")


if __name__ == "__main__":
    main()
