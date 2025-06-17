import logging
import os
from datetime import datetime, timezone
try:
    import pymongo
    from pymongo import MongoClient
    from pymongo.database import Database
    from pymongo.errors import ConnectionFailure, OperationFailure
except ImportError:
    # This allows the script to be imported and parts of it (like SQLite schema)
    # to be understood even if pymongo is not installed in a test environment
    # where only script creation/analysis is happening.
    logging.warning("pymongo library not found. MongoDB functionality will be unavailable.")
    pymongo = None # type: ignore
    MongoClient = None # type: ignore
    Database = None # type: ignore
    ConnectionFailure = None # type: ignore
    OperationFailure = None # type: ignore


# --- Logging Setup ---
LOG_FILE = "db_manager.log"
logger = logging.getLogger(__name__)
if not logger.handlers: # Ensure handlers are not added multiple times
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s")

    # File Handler
    fh = logging.FileHandler(LOG_FILE, mode='w')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # Console Handler (optional, but good for direct script runs)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

# --- SQLite Schema (for documentation) ---
"""
-- RAGAS Table (SQLite Fallback Schema)
-- CREATE TABLE IF NOT EXISTS ragas (
--     Raga TEXT PRIMARY KEY,
--     Raga_URL TEXT,
--     Melakartha_Number INTEGER,
--     Melakartha_Name TEXT,
--     Arohana TEXT,
--     Avarohana TEXT,
--     Raw_Table_Data TEXT,
--     last_updated TEXT -- Store as ISO8601 string (e.g., YYYY-MM-DDTHH:MM:SS.ffffffZ)
-- );

-- AUDIO_FILES Table (SQLite Fallback Schema)
-- This table links audio files to a raga.
-- CREATE TABLE IF NOT EXISTS audio_files (
--     id INTEGER PRIMARY KEY AUTOINCREMENT,
--     raga_name TEXT,                -- Foreign key to RAGAS table
--     original_video_id TEXT,      -- e.g., YouTube video ID
--     original_url TEXT,           -- Full original URL from where it was sourced
--     start_seconds INTEGER,
--     end_seconds INTEGER,
--     downloaded_path TEXT UNIQUE,   -- Filesystem path where the audio is stored
--     download_status TEXT,        -- e.g., "success", "failed", "pending"
--     file_size_bytes INTEGER,
--     duration_seconds REAL,         -- Duration of the downloaded segment/file
--     last_updated TEXT,
--     FOREIGN KEY(raga_name) REFERENCES ragas(Raga)
-- );
"""

# --- MongoDB Implementation ---
def get_db_connection(connection_string: str = "mongodb://localhost:27017/", db_name: str = "carnatic_music_db") -> Database | None:
    """
    Establishes a connection to a MongoDB database.

    Args:
        connection_string (str): The MongoDB connection string.
        db_name (str): The name of the database to connect to.

    Returns:
        pymongo.database.Database | None: The database object if connection is successful, else None.
    """
    if not pymongo:
        logger.error("Pymongo is not installed. Cannot connect to MongoDB.")
        return None

    logger.info(f"Attempting to connect to MongoDB at {connection_string} and use database '{db_name}'")
    try:
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000) # 5-second timeout
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        logger.info("MongoDB connection successful.")
        db = client[db_name]
        return db
    except ConnectionFailure:
        logger.error(f"MongoDB connection failed. Could not connect to server at {connection_string}.")
    except Exception as e:
        logger.error(f"An unexpected error occurred during MongoDB connection: {e}")
    return None

def upsert_raga_data(db: Database, raga_data_dict: dict) -> pymongo.results.UpdateResult | None:
    """
    Inserts or updates a Raga document in the 'ragas' collection.
    The input raga_data_dict is expected to match the new target schema.
    It should include 'Raga', 'Raga_URL', 'Melakartha_Number', 'Melakartha_Name',
    'Arohana', 'Avarohana', 'Raw_Table_Data', and an 'Audio_Files' list.
    Each item in 'Audio_Files' should be a dictionary possibly containing
    'video_id', 'url', 'start_seconds', 'end_seconds', 'downloaded_path', 'status'.
    """
    if not db or not pymongo:
        logger.error("No valid database connection or pymongo library unavailable.")
        return None
    if "Raga" not in raga_data_dict:
        logger.error("Raga data dictionary must contain a 'Raga' field for upsert query.")
        return None

    # Add/update the last_updated timestamp
    raga_data_dict["last_updated"] = datetime.now(timezone.utc)

    query = {"Raga": raga_data_dict["Raga"]}
    update_document = {"$set": raga_data_dict}

    logger.debug(f"Upserting Raga: {raga_data_dict['Raga']}. Query: {query}")
    try:
        ragas_collection = db["ragas"]
        update_result = ragas_collection.update_one(query, update_document, upsert=True)

        if update_result.upserted_id:
            logger.info(f"Inserted new Raga: {raga_data_dict['Raga']} with ID: {update_result.upserted_id}")
        elif update_result.modified_count > 0:
            logger.info(f"Updated existing Raga: {raga_data_dict['Raga']}")
        elif update_result.matched_count > 0:
            logger.info(f"Raga data for {raga_data_dict['Raga']} matched but no changes made (already up-to-date).")
        else:
            # This case should ideally not happen with upsert=True if there's no match (it should insert)
            # unless there was an issue not raising an exception.
            logger.warning(f"Upsert for Raga {raga_data_dict['Raga']} resulted in no known change (matched: {update_result.matched_count}, modified: {update_result.modified_count}).")

        return update_result
    except OperationFailure as e:
        logger.error(f"MongoDB operation failure during upsert for Raga {raga_data_dict.get('Raga', 'N/A')}: {e.details}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during upsert for Raga {raga_data_dict.get('Raga', 'N/A')}: {e}")
    return None


# --- Placeholder SQLite Functions (Optional) ---
def create_sqlite_connection(db_file="carnatic_music.sqlite"):
    """ Creates a database connection to a SQLite database """
    # import sqlite3 # Keep import local if this part is truly optional
    # conn = None
    # try:
    #     conn = sqlite3.connect(db_file)
    #     logger.info(f"SQLite connection established to {db_file}")
    # except Exception as e: # sqlite3.Error
    #     logger.error(f"Error connecting to SQLite database {db_file}: {e}")
    # return conn
    logger.info("SQLite functionality is currently a placeholder.")
    return None

def create_sqlite_tables(conn):
    """ create tables in the SQLite database """
    # try:
    #     # Use the SQL schema comments from above to create tables
    #     logger.info("Creating SQLite tables (placeholder)...")
    #     # Example:
    #     # c = conn.cursor()
    #     # c.execute(RAGAS_TABLE_SQL)
    #     # c.execute(AUDIO_FILES_TABLE_SQL)
    #     # conn.commit()
    # except Exception as e: # sqlite3.Error
    #     logger.error(f"Error creating SQLite tables: {e}")
    pass

def upsert_raga_sqlite(conn, raga_data_dict):
    """ Upsert raga data into SQLite (placeholder) """
    # logger.info(f"Upserting raga data into SQLite for {raga_data_dict.get('Raga')} (placeholder)...")
    # # This would involve:
    # # 1. Upserting into 'ragas' table.
    # # 2. For each audio file in raga_data_dict['Audio_Files'], upserting into 'audio_files' table.
    pass


if __name__ == "__main__":
    logger.info("Running db_manager.py directly for demonstration.")

    # Attempt MongoDB connection
    # In a typical CI/testing environment, a MongoDB instance might not be available.
    # The connection_string might need to be configured via environment variables for real use.
    # For local testing, ensure MongoDB is running at "mongodb://localhost:27017/"

    # Check if MONGO_CONNECTION_STRING is set in environment, otherwise use default
    mongo_uri = os.getenv("MONGO_CONNECTION_STRING", "mongodb://localhost:27017/")
    db_name_env = os.getenv("MONGO_DB_NAME", "carnatic_music_db_demo")

    db_connection = get_db_connection(connection_string=mongo_uri, db_name=db_name_env)

    if db_connection:
        logger.info(f"Successfully connected to MongoDB database '{db_name_env}'.")

        # Sample Raga data dictionary (matching the new target structure)
        sample_raga_data = {
            "Raga": "AbhEri_Demo", # Use a distinct name for demo
            "Raga_URL": "https://ramanarunachalam.github.io/Music/Carnatic/carnatic.html#AbhEri",
            "Melakartha_Number": 22,
            "Melakartha_Name": "Kharaharapriya",
            "Arohana": "S G2 M1 P N2 S",
            "Avarohana": "S N2 D2 P M1 G2 R2 S",
            "Raw_Table_Data": "Melakartha 22 Kharaharapriya Arohana S G2 M1 P N2 S Avarohana S N2 D2 P M1 G2 R2 S",
            "Audio_Files": [ # This list represents audio files *after* processing by downloader
                {
                    "video_id": "sex9LtEWjvg",
                    "url": "https://www.youtube.com/watch?v=sex9LtEWjvg&t=1195s", # Original URL
                    "start_seconds": 1195,
                    "end_seconds": 1215,
                    "downloaded_path": "audio_data/AbhEri_Demo/sex9LtEWjvg_1195_1215.mp3", # Example path
                    "status": "success", # Status from downloader
                    "file_size_bytes": 61000, # Example size
                    "duration_seconds": 20.0 # Example duration
                },
                {
                    "video_id": "GOF1-0dWXmU",
                    "url": "https://www.youtube.com/watch?v=GOF1-0dWXmU",
                    "start_seconds": None,
                    "end_seconds": None,
                    "downloaded_path": "audio_data/AbhEri_Demo/GOF1-0dWXmU_full.mp3", # Example path
                    "status": "success",
                    "file_size_bytes": 3200000,
                    "duration_seconds": 185.0
                },
                 {
                    "video_id": "non_existent_id",
                    "url": "https://www.youtube.com/watch?v=non_existent_id",
                    "start_seconds": None,
                    "end_seconds": None,
                    "downloaded_path": None,
                    "status": "failed" # Example of a failed download
                }
            ]
            # 'last_updated' will be added by upsert_raga_data function
        }

        upsert_result = upsert_raga_data(db_connection, sample_raga_data)

        if upsert_result:
            logger.info(f"Upsert operation for '{sample_raga_data['Raga']}' completed.")
            logger.info(f"  Matched: {upsert_result.matched_count}, Modified: {upsert_result.modified_count}, Upserted ID: {upsert_result.upserted_id}")

            # Clean up: Optionally delete the demo document after test
            # delete_query = {"Raga": sample_raga_data["Raga"]}
            # deleted_count = db_connection["ragas"].delete_one(delete_query).deleted_count
            # logger.info(f"Cleaned up demo document. Deleted count: {deleted_count}")
        else:
            logger.error(f"Upsert operation for '{sample_raga_data['Raga']}' failed or returned no result.")

    else:
        logger.warning("Could not establish MongoDB connection. Skipping demonstration of upsert.")
        logger.info("This is expected if a MongoDB server is not running or accessible at the specified URI.")

    logger.info("db_manager.py demonstration finished.")
