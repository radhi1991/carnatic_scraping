import json
import logging
import re
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException

# --- Constants ---
MAIN_URL = "https://ramanarunachalam.github.io/Music/Carnatic/carnatic.html"
RAGA_LINKS_XPATH = "//div[@id='MENU']//ul[@class='OL_LIST']/li/a"
RAGA_PAGE_HEADING_XPATH = "//div[@id='PAGE_HEADER']//h3[@class='CONTENT_HEADER']/span[@id='PAGE_TITLE']"
RAGA_TABLE_XPATH = "//div[@id='PAGE_VIDEOS']//table"
YOUTUBE_URLS_OL_XPATH = "//div[@id='PAGE_VIDEOS']//div[contains(@class, 'list-group')]/ol"

OUTPUT_JSON_FILE = "refined_raga_data.json"
LOG_FILE = "scraper.log"
DELAY_AFTER_CLICK = 1
WAIT_TIMEOUT = 15
SHORT_WAIT_TIMEOUT = 10
MAX_RAGAS_TO_PROCESS = 3 # Set to 3 for this task

# --- Logging Setup ---
logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s")
    fh = logging.FileHandler(LOG_FILE, mode='w')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

# --- WebDriver Initialization ---
def init_driver():
    logger.debug("Initializing WebDriver.")
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")
    try:
        driver = webdriver.Chrome(options=chrome_options)
        logger.debug("WebDriver initialized successfully.")
        return driver
    except Exception as e:
        logger.critical(f"Failed to initialize WebDriver: {e}")
        raise

# --- Helper Functions ---
def parse_time_param(url_param: str) -> int | None:
    if not url_param or not isinstance(url_param, str): return None
    match = re.match(r"(\d+)(s?)", url_param.strip())
    if match:
        try: return int(match.group(1))
        except ValueError: logger.warning(f"Could not parse time value: {match.group(1)}"); return None
    return None

def parse_youtube_url(raw_url: str) -> dict | None:
    if not raw_url or not isinstance(raw_url, str): return None
    video_id_match = re.search(r"(?:https?://)?(?:www\.)?(?:youtube\.com/(?:watch\?v=|embed/|v/|shorts/)|youtu\.be/)([a-zA-Z0-9_-]{11})", raw_url)
    video_id = video_id_match.group(1) if video_id_match else None
    if not video_id: return None
    start_seconds, end_seconds = None, None
    time_param_match = re.search(r"[?&](?:t|start)=([^&]+)", raw_url)
    if time_param_match: start_seconds = parse_time_param(time_param_match.group(1))
    end_param_match = re.search(r"[?&]end=([^&]+)", raw_url)
    if end_param_match: end_seconds = parse_time_param(end_param_match.group(1))
    return {"video_id": video_id, "start_seconds": start_seconds, "end_seconds": end_seconds, "original_url": raw_url}


def parse_raga_table_data(raw_table_text: str) -> dict:
    logger.debug(f"Attempting to parse table data. Raw text length: {len(raw_table_text)}")
    parsed_data = {"melakartha_number": None, "melakartha_name": None, "arohana": None, "avarohana": None}
    if not raw_table_text or not isinstance(raw_table_text, str): return parsed_data

    melakartha_match = re.search(r"Melakartha\s*(\d+)\s+([\w\s]+?)(?=\s+Arohana|\s+Avarohana|$)", raw_table_text, re.IGNORECASE)
    if melakartha_match:
        try:
            parsed_data["melakartha_number"] = int(melakartha_match.group(1))
            parsed_data["melakartha_name"] = melakartha_match.group(2).strip()
        except Exception: logger.error(f"Error parsing Melakartha from: {raw_table_text[:100]}") # Log snippet on error

    arohana_match = re.search(r"Arohana\s+([srgmpdnSRGMPDN\d\s]+?)(?=\s+Avarohana|$)", raw_table_text, re.IGNORECASE)
    if arohana_match: parsed_data["arohana"] = arohana_match.group(1).strip()

    avarohana_match = re.search(r"Avarohana\s+([srgmpdnSRGMPDN\d\s]+?)(?:\s+Listen|$)", raw_table_text, re.IGNORECASE)
    if avarohana_match: parsed_data["avarohana"] = avarohana_match.group(1).strip()
    else:
        avarohana_fallback_match = re.search(r"Avarohana\s+([srgmpdnSRGMPDN\d\s]+)$", raw_table_text, re.IGNORECASE)
        if avarohana_fallback_match: parsed_data["avarohana"] = avarohana_fallback_match.group(1).strip()
    logger.debug(f"Parsed table result: {parsed_data}")
    return parsed_data

# --- Main Scraping Logic ---
def main():
    logger.info(f"Starting Raga scraper. Will process a maximum of {MAX_RAGAS_TO_PROCESS} raga(s).")
    driver = init_driver()
    all_raga_data = []
    processed_raga_count = 0

    try:
        driver.get(MAIN_URL)
        logger.info(f"Navigated to {MAIN_URL}. Waiting for Raga links.")
        WebDriverWait(driver, WAIT_TIMEOUT).until(EC.presence_of_all_elements_located((By.XPATH, RAGA_LINKS_XPATH)))

        initial_raga_link_elements = driver.find_elements(By.XPATH, RAGA_LINKS_XPATH)
        raga_details_to_process = []
        logger.info(f"Found {len(initial_raga_link_elements)} initial links. Collecting details...")
        for index, el in enumerate(initial_raga_link_elements):
            try:
                raga_name_text = el.text.strip()
                if raga_name_text:
                     raga_details_to_process.append({"name": raga_name_text, "original_index": index})
            except StaleElementReferenceException:
                logger.error(f"Stale element at index {index} during initial scan. Breaking scan.")
                break

        logger.info(f"Collected {len(raga_details_to_process)} Raga names to potentially process.")

        for raga_idx, raga_info in enumerate(raga_details_to_process):
            if processed_raga_count >= MAX_RAGAS_TO_PROCESS:
                logger.info(f"Reached processing limit of {MAX_RAGAS_TO_PROCESS} ragas. Stopping.")
                break

            raga_name = raga_info["name"]
            original_element_index = raga_info["original_index"]
            logger.info(f"--- Processing Raga {raga_idx + 1}/{len(raga_details_to_process)} (Attempt #{processed_raga_count+1}) Name: {raga_name} ---")

            if ',' in raga_name:
                logger.info(f"Skipping Raga '{raga_name}' (contains comma).")
                continue

            current_raga_data = {
                "Raga": raga_name,
                "Raga_URL": MAIN_URL + f"#{raga_name.replace(' ', '_')}",
                "Melakartha_Number": None,
                "Melakartha_Name": None,
                "Arohana": None,
                "Avarohana": None,
                "Audio_URLs": [],
                "Raw_Table_Data": None
            }

            try:
                raga_link_elements_current_iter = driver.find_elements(By.XPATH, RAGA_LINKS_XPATH)
                if original_element_index >= len(raga_link_elements_current_iter):
                    logger.error(f"Link for {raga_name} (idx {original_element_index}) not found. Skipping.")
                    continue
                target_raga_element = raga_link_elements_current_iter[original_element_index]

                driver.execute_script("arguments[0].scrollIntoView(true);", target_raga_element)
                time.sleep(0.5) # Brief pause after scroll
                target_raga_element.click()

                WebDriverWait(driver, WAIT_TIMEOUT).until(EC.text_to_be_present_in_element((By.XPATH, RAGA_PAGE_HEADING_XPATH), raga_name))
                logger.info(f"Raga '{raga_name}' content loaded.")
                time.sleep(DELAY_AFTER_CLICK) # Wait for JS to potentially update more content

                try:
                    table_element = WebDriverWait(driver, SHORT_WAIT_TIMEOUT).until(EC.presence_of_element_located((By.XPATH, RAGA_TABLE_XPATH)))
                    raw_table_text = table_element.text
                    current_raga_data["Raw_Table_Data"] = raw_table_text # Store raw table text
                    if raw_table_text:
                        logger.debug(f"Table text found for {raga_name} (len {len(raw_table_text)}). Parsing...")
                        parsed_table = parse_raga_table_data(raw_table_text)
                        # Populate new structure directly
                        current_raga_data["Melakartha_Number"] = parsed_table.get("melakartha_number")
                        current_raga_data["Melakartha_Name"] = parsed_table.get("melakartha_name")
                        current_raga_data["Arohana"] = parsed_table.get("arohana")
                        current_raga_data["Avarohana"] = parsed_table.get("avarohana")
                        logger.debug(f"Stored parsed table data for {raga_name}")
                except TimeoutException:
                    logger.warning(f"Details table not found for Raga: {raga_name}")

                try:
                    youtube_list_ol = WebDriverWait(driver, SHORT_WAIT_TIMEOUT).until(EC.presence_of_element_located((By.XPATH, YOUTUBE_URLS_OL_XPATH)))
                    youtube_a_elements = youtube_list_ol.find_elements(By.TAG_NAME, "a")
                    logger.debug(f"Found {len(youtube_a_elements)} YouTube link elements for {raga_name}.")
                    for link_el in youtube_a_elements:
                        raw_url_val = link_el.get_attribute('href')
                        if raw_url_val:
                            parsed_yt_url = parse_youtube_url(raw_url_val)
                            if parsed_yt_url:
                                # Map 'original_url' to 'url' for the new structure
                                current_raga_data["Audio_URLs"].append({
                                    "video_id": parsed_yt_url["video_id"],
                                    "url": parsed_yt_url["original_url"], # Key changed here
                                    "start_seconds": parsed_yt_url["start_seconds"],
                                    "end_seconds": parsed_yt_url["end_seconds"]
                                })
                except TimeoutException:
                    logger.info(f"YouTube URL list not found for Raga: {raga_name}")

                all_raga_data.append(current_raga_data)
                processed_raga_count += 1
            except Exception as e: logger.error(f"Error processing Raga {raga_name}: {e}", exc_info=True)
            logger.info(f"Finished Raga: {raga_name}. Processed count: {processed_raga_count}/{MAX_RAGAS_TO_PROCESS}")

    except Exception as e: logger.critical(f"Critical error in main: {e}", exc_info=True)
    finally:
        if driver: driver.quit(); logger.info("WebDriver closed.")
        with open(OUTPUT_JSON_FILE, 'w', encoding='utf-8') as f: json.dump(all_raga_data, f, indent=4, ensure_ascii=False)
        logger.info(f"Saved data for {len(all_raga_data)} Ragas to {OUTPUT_JSON_FILE}")
        if not all_raga_data and MAX_RAGAS_TO_PROCESS > 0 : logger.warning("No Raga data was collected despite attempts.")

if __name__ == "__main__":
    main()
