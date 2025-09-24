"""
Logger Setup Script
File: utils/utils_logger.py

This script provides logging functions for the project.
Logging is an essential way to track events and issues during execution.

Features:
- Logs information, warnings, and errors to a designated log file.
- Ensures the log directory exists.
- Sanitizes logs to remove personal/identifying information for GitHub sharing.
"""

#####################################
# Import Modules
#####################################

# Imports from Python Standard Library
import os
import pathlib
import getpass
import sys
from typing import Mapping, Any

# Imports from external packages
from loguru import logger

#####################################
# Helper: sanitize + format
#####################################

def sanitize_message(record: Mapping[str, Any]) -> str:
    """Remove personal/identifying information from log messages and escape braces."""
    message = record["message"]

    # Replace username with generic placeholder
    try:
        current_user = getpass.getuser()
        message = message.replace(current_user, "USER")
    except Exception:
        pass

    # Replace home directory paths
    try:
        home_path = str(pathlib.Path.home())
        message = message.replace(home_path, "~")
    except Exception:
        pass

    # Replace absolute paths with relative ones
    try:
        cwd = str(pathlib.Path.cwd())
        message = message.replace(cwd, "PROJECT_ROOT")
    except Exception:
        pass

    # Normalize slashes
    message = message.replace("\\", "/")

    # Escape braces so Loguru won't treat them as fields
    message = message.replace("{", "{{").replace("}", "}}")

    return message


def format_sanitized(record: Mapping[str, Any]) -> str:
    """Custom formatter that sanitizes messages and returns a plain string."""
    message = sanitize_message(record)
    time_str = record["time"].strftime("%Y-%m-%d %H:%M:%S")
    level_name = record["level"].name
    return f"{time_str} | {level_name} | {message}\n"


#####################################
# Logging setup (per-PID files; no rotation)
#####################################

LOG_FOLDER: pathlib.Path = pathlib.Path("logs")
LOG_FOLDER.mkdir(exist_ok=True)

# One log file per process to avoid Windows rename collisions
PID = os.getpid()
LOG_FILE: pathlib.Path = LOG_FOLDER / f"project_log_{PID}.log"

try:
    logger.remove()

    # File sink: no rotation/retention to avoid Windows rename contention
    logger.add(
        LOG_FILE,
        level="INFO",
        enqueue=True,          # multiprocess-safe queue
        format=format_sanitized,
        # rotation=None,       # default None
        # retention=None,      # default None
    )

    # Console sink (stderr). If you want fewer logs on screen,
    # comment this out in either the producer or consumer.
    logger.add(
        sys.stderr,
        level="INFO",
        enqueue=True,
        format=format_sanitized,
    )

    logger.info(f"Logging to file: {LOG_FILE}")
    logger.info("Log sanitization enabled, personal info removed")

except Exception as e:
    # Fallback: ensure we still log to stderr if file sink fails
    try:
        logger.remove()
        logger.add(sys.stderr, level="INFO", enqueue=True, format=format_sanitized)
        logger.error(f"Error configuring file logger, using stderr only: {e}")
    except Exception:
        pass


#####################################
# Convenience helpers (unchanged)
#####################################

def get_log_file_path() -> pathlib.Path:
    """Return the path to the log file."""
    return LOG_FILE


def log_example() -> None:
    """Example logging function to demonstrate logging behavior."""
    try:
        logger.info("This is an example info message.")
        logger.info(f"Current working directory: {pathlib.Path.cwd()}")
        logger.info(f"User home directory: {pathlib.Path.home()}")
        logger.warning("This is an example warning message.")
        logger.error("This is an example error message.")
    except Exception as e:
        logger.error(f"An error occurred during logging: {e}")


#####################################
# Main for quick manual test (optional)
#####################################

def main() -> None:
    logger.info("STARTING utils_logger.py")
    log_example()
    logger.info(f"View the log output at {LOG_FILE}")
    logger.info("EXITING utils_logger.py")


if __name__ == "__main__":
    main()
