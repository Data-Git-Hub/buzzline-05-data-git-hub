"""
file_emitter.py

Emit messages to a local file sink.

A file emitter is the simplest option:
- Always available, no external services required.
- Produces a line-delimited JSON log (JSONL).
- Easy to inspect, replay, or share with others.

Use this when debugging or when a lightweight
append-only log is enough for your use case.
"""


import json
import pathlib
from typing import Mapping, Any

from utils.utils_logger import logger


def emit_message(message: Mapping[str, Any], *, path: pathlib.Path) -> bool:
    """
    Append a JSON-encoded message to a local file.

    Args:
        message (Mapping[str, Any]): The message to be written.
        path (pathlib.Path): The file path where messages are appended.

    Returns:
        bool: True if write succeeded, False otherwise.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(message) + "\n")
        logger.debug(f"Wrote message to {path}")
        return True
    except Exception as e:
        logger.error(f"File emit failed: {e}")
        return False