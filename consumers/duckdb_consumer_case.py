"""
duckdb_consumer_case.py

Functions:
- init_db(db_path: pathlib.Path): Initialize (or recreate) the 'streamed_messages' table.
- insert_message(message: dict, db_path: pathlib.Path): Insert one processed message.
- delete_message(message_id: int, db_path: pathlib.Path): Delete by row id.

This mirrors the SQLite version for easy comparison.
"""

#####################################
# Import Modules
#####################################

# standard library
import os
import pathlib

# external
import duckdb

# local
from utils.utils_logger import logger


#####################################
# Initialize DuckDB Database / Table
#####################################


def init_db(db_path: pathlib.Path) -> None:
    """
    Create (or recreate) the streamed_messages table in a DuckDB database.

    Args:
        db_path: Path to the DuckDB file (e.g., Path("data/buzz.duckdb"))
    """
    logger.info("Calling DuckDB init_db() with {db_path=}.")
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        # Connect to (or create) the DuckDB file
        con: duckdb.DuckDBPyConnection = duckdb.connect(str(db_path))
        logger.info("SUCCESS: Connected to DuckDB.")

        # For a fresh start, drop then create
        con.execute("DROP TABLE IF EXISTS streamed_messages;")
        con.execute(
            """
            CREATE TABLE streamed_messages (
                id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, -- auto-increment
                message TEXT,
                author TEXT,
                timestamp TEXT,
                category TEXT,
                sentiment DOUBLE,
                keyword_mentioned TEXT,
                message_length INTEGER
            );
            """
        )
        logger.info(f"SUCCESS: Table 'streamed_messages' ready at {db_path}.")
        con.close()
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize DuckDB at {db_path}: {e}")


#####################################
# Insert One Message
#####################################


def insert_message(message: dict, db_path: pathlib.Path) -> None:
    """
    Insert a single processed message into DuckDB.

    Args:
        message: dict with keys matching table columns
        db_path: Path to the DuckDB file
    """
    logger.info("Calling DuckDB insert_message() with:")
    logger.info(f"{message=}")
    logger.info(f"{db_path=}")

    try:
        con: duckdb.DuckDBPyConnection = duckdb.connect(str(db_path))
        con.execute(
            """
            INSERT INTO streamed_messages
                (message, author, timestamp, category, sentiment, keyword_mentioned, message_length)
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            [
                message["message"],
                message["author"],
                message["timestamp"],
                message["category"],
                float(message["sentiment"]),
                message["keyword_mentioned"],
                int(message["message_length"]),
            ],
        )
        logger.info("Inserted one message into DuckDB.")
        con.close()
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into DuckDB: {e}")


#####################################
# Delete By ID
#####################################


def delete_message(message_id: int, db_path: pathlib.Path) -> None:
    """
    Delete a message from DuckDB by id.

    Args:
        message_id: row id to delete
        db_path: Path to the DuckDB file
    """
    try:
        con = duckdb.connect(str(db_path))
        con.execute("DELETE FROM streamed_messages WHERE id = ?;", [message_id])
        logger.info(f"Deleted message with id {message_id} from DuckDB.")
        con.close()
    except Exception as e:
        logger.error(f"ERROR: Failed to delete message from DuckDB: {e}")


#####################################
# Local test runner (optional)
#####################################


def _resolve_duckdb_path() -> pathlib.Path:
    """
    Resolve a DuckDB file path using utils_config if present,
    else fall back to env/default: data/buzz.duckdb
    """
    # Try utils.utils_config first (keeps parity with your SQLite module)
    try:
        import utils.utils_config as config  # type: ignore

        if hasattr(config, "get_duckdb_path"):
            return config.get_duckdb_path()  # expected to return pathlib.Path
    except Exception:
        pass

    # Fallback to environment/defaults
    base_dir = os.getenv("BASE_DATA_DIR", "data")
    file_name = os.getenv("DUCKDB_DB_FILE_NAME", "buzz.duckdb")
    return pathlib.Path(base_dir).joinpath(file_name)


def main() -> None:
    """
    Local smoke test:
    - init_db()
    - insert_message()
    - SELECT to verify row exists
    - delete_message() to clean up
    """
    logger.info("Starting DuckDB db testing.")
    db_path = _resolve_duckdb_path()
    logger.info(f"Using DuckDB file at: {db_path}")

    # 1) Init
    init_db(db_path)

    # 2) Insert a sample message
    test_message = {
        "message": "I just shared a meme! It was amazing.",
        "author": "Charlie",
        "timestamp": "2025-01-29 14:35:20",
        "category": "humor",
        "sentiment": 0.87,
        "keyword_mentioned": "meme",
        "message_length": 42,
    }
    insert_message(test_message, db_path)

    # 3) Verify via SELECT
    try:
        con = duckdb.connect(str(db_path))
        row = con.execute(
            """
            SELECT id, message, author
            FROM streamed_messages
            WHERE message = ? AND author = ?
            ORDER BY id DESC
            LIMIT 1;
            """,
            [test_message["message"], test_message["author"]],
        ).fetchone()
        con.close()

        if row:
            inserted_id = row[0]
            logger.info(f"Verified insert. Latest row id={inserted_id}")

            # 4) Clean up the sample row
            delete_message(inserted_id, db_path)
        else:
            logger.warning("Test row not found; nothing to delete.")
    except Exception as e:
        logger.error(f"ERROR: Verification SELECT failed: {e}")

    logger.info("Finished DuckDB db testing.")


#####################################
# Conditional execution
#####################################

if __name__ == "__main__":
    main()
