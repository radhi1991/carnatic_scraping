import json
import logging
import os
import re
from datetime import datetime, timezone # For consistency if db_manager adds it

# Attempt to import from db_manager
try:
    from db_manager import get_db_connection, upsert_raga_data, pymongo
    # pymongo is imported to check its availability for type hints if needed,
    # and to potentially handle MongoDB specific errors if db_manager doesn't fully abstract them.
except ImportError:
    logging.critical("Failed to import from db_manager.py. Ensure the file exists and is in PYTHONPATH.")
    # Define placeholders if db_manager is not available, to allow basic script structure check
    # This is mostly for environments where full execution isn't the goal.
    def get_db_connection(*args, **kwargs): logging.error("db_manager.get_db_connection not available."); return None
    def upsert_raga_data(*args, **kwargs): logging.error("db_manager.upsert_raga_data not available."); return None
    pymongo = None


# --- Constants ---
REFINED_RAGA_JSON_FILE = "refined_raga_data.json"
DOWNLOAD_SUMMARY_JSON_FILE = "download_summary.json"
BASE_AUDIO_DIR = "audio_data/" # Must match audio_downloader.py's constant
LOG_FILE = "database_populator.log"

# --- Logging Setup ---
logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s")

    fh = logging.FileHandler(LOG_FILE, mode='w')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

# --- Helper Functions ---
def sanitize_filename_component(name_part: str) -> str:
    """Sanitizes a string component to be safe for directory or file names."""
    if not name_part: return "_"
    name_part = str(name_part)
    name_part = re.sub(r'[^\w\s-]', '', name_part) # Keep alphanumeric, whitespace, hyphens
    name_part = re.sub(r'\s+', '_', name_part).strip('_') # Replace whitespace with underscore
    return name_part if name_part else "_"

def construct_expected_filepath(raga_name: str, video_id: str, start_seconds: int | None, end_seconds: int | None) -> str:
    """
    Constructs the expected filepath for a downloaded audio segment,
    mirroring the logic in audio_downloader.py.
    Returns a relative path.
    """
    sanitized_raga_name = sanitize_filename_component(raga_name)
    segment_name_part = sanitize_filename_component(video_id)

    if start_seconds is not None and end_seconds is not None:
        segment_name_part += f"_{start_seconds}_{end_seconds}"
    elif start_seconds is not None:
        segment_name_part += f"_{start_seconds}_inf"
    else:
        segment_name_part += "_full"

    output_filename = f"{segment_name_part}.mp3"
    # This creates a relative path, e.g., "audio_data/Raga_Name/filename.mp3"
    return os.path.join(BASE_AUDIO_DIR, sanitized_raga_name, output_filename)

# --- Main Logic ---
def main():
    logger.info("Starting Database Populator script.")

    # Load Raga data
    try:
        with open(REFINED_RAGA_JSON_FILE, 'r', encoding='utf-8') as f:
            all_raga_info = json.load(f)
        logger.info(f"Successfully loaded {len(all_raga_info)} raga entries from {REFINED_RAGA_JSON_FILE}.")
    except FileNotFoundError:
        logger.critical(f"Input file {REFINED_RAGA_JSON_FILE} not found. Exiting.")
        return
    except json.JSONDecodeError:
        logger.critical(f"Error decoding JSON from {REFINED_RAGA_JSON_FILE}. Exiting.")
        return

    # Load Download Summary
    successful_download_paths = set()
    try:
        with open(DOWNLOAD_SUMMARY_JSON_FILE, 'r', encoding='utf-8') as f:
            download_summary = json.load(f)
        # The key in download_summary.json was "successful_downloads" not "successful"
        # successful_download_paths = set(download_summary.get("successful", []))
        successful_download_paths = set(download_summary.get("successful_downloads", []))

        logger.info(f"Successfully loaded download summary. Found {len(successful_download_paths)} successful downloads.")
        logger.debug(f"Successful download paths: {successful_download_paths}")
    except FileNotFoundError:
        logger.warning(f"Download summary file {DOWNLOAD_SUMMARY_JSON_FILE} not found. Assuming no files were successfully downloaded.")
    except json.JSONDecodeError:
        logger.warning(f"Error decoding JSON from {DOWNLOAD_SUMMARY_JSON_FILE}. Assuming no successful downloads.")

    # Attempt DB Connection
    # Use environment variables for connection string if available, otherwise default.
    mongo_uri = os.getenv("MONGO_CONNECTION_STRING", "mongodb://localhost:27017/")
    db_name_env = os.getenv("MONGO_DB_NAME", "carnatic_music_db") # Using the main DB name now

    db = None
    if pymongo: # Check if pymongo was imported successfully
        db = get_db_connection(connection_string=mongo_uri, db_name=db_name_env)
    else:
        logger.warning("Pymongo library not available, MongoDB operations will be skipped.")


    if not db:
        logger.warning("Proceeding without database connection. Data will be prepared but not inserted/updated.")

    processed_count = 0
    upserted_count = 0
    updated_count = 0

    for raga_entry in all_raga_info:
        raga_name = raga_entry.get("Raga")
        if not raga_name:
            logger.warning("Skipping raga entry due to missing 'Raga' name.")
            continue

        logger.info(f"Processing Raga: {raga_name}")

        # Construct the document for the database
        raga_document_for_db = {
            "Raga": raga_name,
            "Raga_URL": raga_entry.get("Raga_URL"),
            "Melakartha_Number": raga_entry.get("Melakartha_Number"),
            "Melakartha_Name": raga_entry.get("Melakartha_Name"),
            "Arohana": raga_entry.get("Arohana"),
            "Avarohana": raga_entry.get("Avarohana"),
            "Raw_Table_Data": raga_entry.get("Raw_Table_Data"),
            "Audio_Files": [],
            # last_updated will be added by db_manager.upsert_raga_data
        }

        # Process Audio_URLs to create Audio_Files list
        for audio_url_info in raga_entry.get("Audio_URLs", []):
            video_id = audio_url_info.get("video_id")
            original_url = audio_url_info.get("url") # This is original_url from raga_scraper output
            start_seconds = audio_url_info.get("start_seconds")
            end_seconds = audio_url_info.get("end_seconds")

            if not video_id or not original_url:
                logger.warning(f"Skipping audio entry for Raga {raga_name} due to missing video_id or url.")
                continue

            expected_path = construct_expected_filepath(raga_name, video_id, start_seconds, end_seconds)
            logger.debug(f"For {raga_name} - {video_id}, expected path: {expected_path}")

            audio_file_doc = {
                "original_video_id": video_id,
                "original_url": original_url,
                "start_seconds": start_seconds,
                "end_seconds": end_seconds,
                "downloaded_path": None, # Relative path for DB
                "download_status": "failed", # Default to failed
                # file_size_bytes and duration_seconds could be added later if available
            }

            if expected_path in successful_download_paths:
                audio_file_doc["download_status"] = "success"
                audio_file_doc["downloaded_path"] = expected_path # Store the relative path
                logger.debug(f"Marked {expected_path} as 'success' for Raga {raga_name}.")
            else:
                logger.debug(f"Marked {expected_path} as 'failed' (not in successful_downloads set) for Raga {raga_name}.")

            raga_document_for_db["Audio_Files"].append(audio_file_doc)

        logger.debug(f"Prepared document for Raga '{raga_name}': {json.dumps(raga_document_for_db, indent=2, default=str)}")

        if db:
            logger.debug(f"Attempting to upsert data for Raga: {raga_name}")
            upsert_result = upsert_raga_data(db, raga_document_for_db)
            if upsert_result:
                if upsert_result.upserted_id:
                    upserted_count +=1
                elif upsert_result.modified_count > 0:
                    updated_count += 1
                logger.info(f"DB operation for {raga_name} complete. Matched: {upsert_result.matched_count}, Modified: {upsert_result.modified_count}, UpsertedID: {upsert_result.upserted_id}")
            else:
                logger.error(f"DB upsert failed for Raga: {raga_name}")
        else:
            logger.info(f"No DB connection. Skipping DB upsert for Raga: {raga_name}.")

        processed_count += 1

    logger.info("--- Database Population Summary ---")
    logger.info(f"Total Raga entries processed from JSON: {processed_count}")
    if db:
        logger.info(f"New Ragas inserted: {upserted_count}")
        logger.info(f"Existing Ragas updated: {updated_count}")
    else:
        logger.info("Database operations were skipped due to no connection.")
    logger.info("Database Populator script finished.")

if __name__ == "__main__":
    main()
