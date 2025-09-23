"""
utils/emitters/__init__.py

Emitters send messages from a source to one or more sinks.

- A *source* generates data (e.g., a message generator).
- A *sink* stores or transports that data (e.g., file, Kafka, database).
- An *emitter* is the callable bridge that takes messages from a source
  and writes them to a chosen sink.

This package provides multiple emitter modules:
- file_emitter.py    - append messages to a local JSONL file
- kafka_emitter.py   - publish messages to a Kafka topic
- sqlite_emitter.py  - insert messages into a SQLite database
- duckdb_emitter.py  - insert messages into a DuckDB database

Choose an emitter based on purpose:
- file: simplest, always works, easy to inspect
- kafka: distributed streaming and decoupling producers/consumers
- sqlite: relational store for small projects, widely used in browsers, etc.
- duckdb: analytical queries and OLAP workflows on local data

Each emitter defines a single `emit_message()` function.
This modular structure keeps sinks independent, composable,
and easy to swap or extend in streaming pipelines.
"""

from . import file_emitter
from . import kafka_emitter 
from . import sqlite_emitter
from . import duckdb_emitter

__all__ = [
    "file_emitter",
    "kafka_emitter",
    "sqlite_emitter",
    "duckdb_emitter",
]
 
