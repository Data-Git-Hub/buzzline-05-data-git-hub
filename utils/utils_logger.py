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
import pathlib
import getpass
import sys
from typing import Mapping, Any

# Imports from external packages
from loguru import logger

#####################################
# Default Configurations
#####################################

# Get this file name without the extension
CURRENT_SCRIPT = pathlib.Path(__file__).stem

# Set directory where logs will be stored
LOG_FOLDER: pathlib.Path = pathlib.Path("logs")

# Set the name of the log file
LOG_FILE: pathlib.Path = LOG_FOLDER.joinpath("project_log.log")

#####################################
# Helper Functions
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

    # Replace Windows backslashes with forward slashes for consistency
    message = message.replace("\\", "/")

    # IMPORTANT: Escape braces so Loguru's string formatter won't treat them as fields
    # This preserves your original "{time} | {level} | {message}" format safely.
    message = message.replace("{", "{{").replace("}", "}}")

    # Return the sanitized message without modifying the record
    return message


def format_sanitized(record: Mapping[str, Any]) -> str:
    """Custom formatter that sanitizes messages and returns a plain string."""
    message = sanitize_message(record)
    time_str = record["time"].strftime("%Y-%m-%d %H:%M:%S")
    level_name = record["level"].name
    return f"{time_str} | {level_name} | {message}\n"


try:
    LOG_FOLDER.mkdir(exist_ok=True)
    print(f"Log folder ready at: {LOG_FOLDER}")
except Exception as e:
    print(f"Error creating log folder: {e}")

try:
    logger.remove()
    logger.add(
        LOG_FILE,
        level="INFO",
        rotation="50 kB",  # Small files
        retention=1,  # Keep last rotated file
        compression=None,  # No compression needed
        enqueue=True,  # safer across threads/processes
        format=format_sanitized,
    )
    logger.add(
        sys.stderr,
        level="INFO",
        enqueue=True,
        format=format_sanitized,
    )
    logger.info(f"Logging to file: {LOG_FILE}")
    logger.info("Log sanitization enabled, personal info removed")
except Exception as e:
    logger.error(f"Error configuring logger to write to file: {e}")


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
# Main Function for Testing
#####################################


def main() -> None:
    """Main function to execute logger setup and demonstrate its usage."""
    logger.info(f"STARTING {CURRENT_SCRIPT}.py")

    # Call the example logging function
    log_example()

    logger.info(f"View the log output at {LOG_FILE}")
    logger.info(f"EXITING {CURRENT_SCRIPT}.py.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
