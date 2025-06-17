import json
import os
import subprocess
import logging
import re
import time # Import time for the timeout test

# --- Constants ---
INPUT_JSON_FILE = "refined_raga_data.json"
BASE_AUDIO_DIR = "audio_data/"
DOWNLOAD_SUMMARY_FILE = "download_summary.json"
LOG_FILE = "audio_downloader.log"
SUBPROCESS_TIMEOUT = 180 # Seconds for yt-dlp to complete

# --- Logging Setup ---
logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.setLevel(logging.DEBUG) # Set to DEBUG for this test
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s")
    fh = logging.FileHandler(LOG_FILE, mode='w')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

def sanitize_filename_component(name_part: str) -> str:
    if not name_part: return "_"
    name_part = str(name_part)
    name_part = re.sub(r'[^\w\s-]', '', name_part)
    name_part = re.sub(r'\s+', '_', name_part).strip('_')
    return name_part if name_part else "_"

def download_audio_segment(raga_name: str, video_id: str, video_url: str, start_seconds: int | None, end_seconds: int | None) -> tuple[bool, str | None]:
    logger.debug(f"Inside download_audio_segment for {raga_name} - {video_id}")
    sanitized_raga_name = sanitize_filename_component(raga_name)
    output_raga_dir = os.path.join(BASE_AUDIO_DIR, sanitized_raga_name)

    try:
        if not os.path.exists(output_raga_dir):
            os.makedirs(output_raga_dir)
            logger.info(f"Created directory: {output_raga_dir}")
    except OSError as e:
        logger.error(f"Failed to create directory {output_raga_dir}: {e}")
        return False, None

    segment_name_part = sanitize_filename_component(video_id)
    download_section_args = []

    if start_seconds is not None and end_seconds is not None:
        segment_name_part += f"_{start_seconds}_{end_seconds}"
        download_section_args = ["--download-sections", f"*{start_seconds}-{end_seconds}"]
    elif start_seconds is not None:
        segment_name_part += f"_{start_seconds}_inf"
        download_section_args = ["--download-sections", f"*{start_seconds}-inf"]
    else:
        segment_name_part += "_full"

    output_filename = f"{segment_name_part}.mp3"
    output_filepath = os.path.abspath(os.path.join(output_raga_dir, output_filename))

    yt_dlp_command = [
        "yt-dlp", "-x", "--audio-format", "mp3",
        "--no-warnings",
        "--no-playlist",
        "-o", output_filepath
    ]

    if download_section_args:
        yt_dlp_command.extend(download_section_args)

    yt_dlp_command.append(video_url)

    logger.info(f"Attempting download for Raga '{raga_name}', Video ID '{video_id}'")
    logger.info(f"Full yt-dlp command list: {yt_dlp_command}")
    logger.debug(f"Executing command string for copy-paste: {' '.join(map(str, yt_dlp_command))}")

    try:
        logger.debug(f"Calling subprocess.run with timeout={SUBPROCESS_TIMEOUT}s, without capturing output.")
        process = subprocess.run(yt_dlp_command, check=False, timeout=SUBPROCESS_TIMEOUT) # Added timeout
        logger.debug(f"subprocess.run finished. Return code: {process.returncode}")

        if process.returncode == 0:
            if os.path.exists(output_filepath):
                logger.info(f"Successfully downloaded: {output_filepath}")
                return True, output_filepath
            else:
                logger.error(f"yt-dlp indicated success (retcode 0) for {video_url} but output file {output_filepath} not found.")
                return False, None
        else:
            logger.error(f"Download failed for {video_url}. Return code: {process.returncode}")
            if os.path.exists(output_filepath):
                try: os.remove(output_filepath)
                except OSError: logger.warning(f"Could not remove partially downloaded file {output_filepath}")
            return False, None
    except FileNotFoundError:
        logger.critical("yt-dlp command not found. Ensure it's installed and in PATH.")
        return False, None
    except subprocess.TimeoutExpired:
        logger.error(f"yt-dlp command timed out after {SUBPROCESS_TIMEOUT} seconds for URL: {video_url}")
        # Clean up potentially partial file if timeout occurred
        if os.path.exists(output_filepath):
            logger.info(f"Attempting to remove potentially partial file: {output_filepath} due to timeout.")
            try: os.remove(output_filepath); logger.info("Partial file removed.")
            except OSError as e_rm: logger.warning(f"Could not remove partial file {output_filepath}: {e_rm}")
        return False, None
    except Exception as e:
        logger.error(f"An unexpected error during subprocess execution for {video_url}: {e}", exc_info=True)
        return False, None

def main(): # Logic largely unchanged, still processes 1st raga's 1st audio
    logger.info("Starting audio downloader script. Will attempt to download one audio file total.")
    successful_downloads, failed_downloads = [], []
    total_downloads_attempted = 0

    try:
        with open(INPUT_JSON_FILE, 'r', encoding='utf-8') as f: ragas_data = json.load(f)
    except FileNotFoundError: logger.critical(f"'{INPUT_JSON_FILE}' not found. Exiting."); return
    except json.JSONDecodeError: logger.critical(f"Error decoding JSON from '{INPUT_JSON_FILE}'. Exiting."); return

    if not os.path.exists(BASE_AUDIO_DIR):
        try: os.makedirs(BASE_AUDIO_DIR); logger.info(f"Base audio directory created: {BASE_AUDIO_DIR}")
        except OSError as e: logger.critical(f"Failed to create {BASE_AUDIO_DIR}: {e}. Exiting."); return

    for raga_entry in ragas_data:
        if total_downloads_attempted >= 1:
            logger.info("One download already attempted. Stopping further processing.")
            break

        raga_name = raga_entry.get("Raga")
        audio_urls = raga_entry.get("Audio_URLs", [])
        if not raga_name:
            logger.warning("Raga entry missing name. Skipping this entry.")
            continue

        logger.info(f"Considering Raga: {raga_name}")
        if not audio_urls:
            logger.info(f"No audio URLs for Raga: {raga_name}. Looking for next raga with audio.")
            continue

        audio_url_info = audio_urls[0]
        video_id, video_url_str = audio_url_info.get("video_id"), audio_url_info.get("url")
        start_seconds, end_seconds = audio_url_info.get("start_seconds"), audio_url_info.get("end_seconds")

        if not video_id or not video_url_str:
            logger.warning(f"Missing video_id/URL for first audio in Raga {raga_name}. Skipping this audio entry.");
            total_downloads_attempted = 1
            continue

        success, filepath = download_audio_segment(raga_name, video_id, video_url_str, start_seconds, end_seconds)
        if success and filepath: successful_downloads.append(filepath)
        else: failed_downloads.append(video_url_str)

        total_downloads_attempted = 1

    logger.info("Download process finished.")
    logger.info(f"Successfully downloaded: {len(successful_downloads)} file(s): {successful_downloads}")
    logger.info(f"Failed to download: {len(failed_downloads)} file(s): {failed_downloads}")

    summary_data = {"successful_downloads": successful_downloads, "failed_downloads": failed_downloads}
    try:
        with open(DOWNLOAD_SUMMARY_FILE, 'w', encoding='utf-8') as f: json.dump(summary_data, f, indent=4)
        logger.info(f"Download summary saved to: {DOWNLOAD_SUMMARY_FILE}")
    except Exception as e: logger.error(f"Failed to save download summary: {e}")

if __name__ == "__main__":
    main()
