# -*- coding: utf-8 -*-
# """
# src/sandbox_plaid/get_item.py
# Created on June 03, 2025
# @ Author: Mazhar
# """


import json
import logging
import os
import sys
import time
from typing import Any, Optional

import requests
from dotenv import load_dotenv

# Construct the path to the project root directory
PROJECT_ROOT: str = os.path.abspath(
    path=os.path.join(os.path.dirname(p=__file__), os.pardir, os.pardir)
)
# print(f"Project root: {PROJECT_ROOT}")

# Add the project root to the Python path
sys.path.insert(0, PROJECT_ROOT)

# Import configs from the configs folder
from configs.configs import cfgs_plaid_sbx

# Import the logger setup
from configs.logging_setup import get_logger

# Get the logger
logger: logging.Logger = get_logger()

from utils.data_to_json import save_all_item_data_to_json
from utils.delete_files import delete_files_if_exist

# Import Utils Functions
from utils.fetch_response import fetch_response
from utils.get_access_token import get_plaid_access_token
from utils.get_cols_series import get_col_series_from_csv
from utils.json_to_csv import save_available_products_to_csv

logger.info(msg="=== Running `src/sandbox_plaid/get_item.py`")
# logger.info(msg=f"Project root: {PROJECT_ROOT}")
# logger.info(msg=f"Python path: {sys.path}")
# logger.info(msg=f"Configuration: {cfgs_plaid_sbx}")

# --- Configuration and Environment Variables ---
cfgs: dict[str, Any] = cfgs_plaid_sbx

ENV_FILE_PATH: str | None = cfgs["ENV_FILE_PATH"]

load_dotenv(dotenv_path=ENV_FILE_PATH)
CLIENT_ID: str | None = os.getenv(key="PLAID_CLIENT_ID")
SECRET: str | None = os.getenv(key="PLAID_CLIENT_SECRET")
OVERRIDE_USERNAME: str | None = os.getenv(key="PLAID_LINK_USERNAME")
OVERRIDE_PASSWORD: str | None = os.getenv(key="PLAID_LINK_PASSWORD")

DATA_DIR: str | None = cfgs["DATA_DIR"]
if DATA_DIR is not None:
    dir_path: str = os.path.dirname(DATA_DIR)
    if not os.path.exists(path=dir_path):
        os.makedirs(name=dir_path)
else:
    logger.info(msg="Error: DATA_DIR missing in YAML CONFIG FILE")
    exit(1)

if not all(
    [
        CLIENT_ID,
        SECRET,
        DATA_DIR,
        cfgs,
    ]
):
    logger.critical("Critical configurations missing. Exiting.")
    sys.exit(1)

JSON_FILE_PATH: str | None = os.path.join(DATA_DIR, "items.json") if DATA_DIR else None
CSV_FILE_PATH: str | None = os.path.join(DATA_DIR, "items.csv") if DATA_DIR else None
INSTITUTION_FILE_PATH: Optional[str] = (
    os.path.join(DATA_DIR, "institutions.csv") if DATA_DIR else None
)

logger.info(msg=f"JSON_FILE_PATH: {JSON_FILE_PATH}")
logger.info(msg=f"CSV_FILE_PATH: {CSV_FILE_PATH}")
logger.info(msg=f"Institution CSV file path: {INSTITUTION_FILE_PATH}")

# Delete JSON_FILE_PATH and CSV_FILE_PATH if they exist to ensure fresh data
files_to_delete: list[Optional[str]] = [JSON_FILE_PATH, CSV_FILE_PATH]
delete_files_if_exist(file_paths=files_to_delete, logger=logger)

INSTITUTION_IDS_TO_PROCESS: list[str] = []  # Initialize
if INSTITUTION_FILE_PATH and os.path.exists(path=INSTITUTION_FILE_PATH):
    INSTITUTION_IDS_TO_PROCESS = get_col_series_from_csv(
        csv_filepath=INSTITUTION_FILE_PATH, column_name="institution_id"
    )
    if INSTITUTION_IDS_TO_PROCESS:
        logger.info(msg=f"Institution IDs to process: {INSTITUTION_IDS_TO_PROCESS}")
    else:
        logger.warning(
            msg=f"No institution IDs found in {INSTITUTION_FILE_PATH}. Halting."
        )
        sys.exit(0)  # Or handle as appropriate
else:
    logger.error(
        msg=f"Institution CSV file not found at {INSTITUTION_FILE_PATH} or path not configured. Halting."
    )
    sys.exit(1)


INITIAL_PRODUCTS: list[str] = ["transactions"]  # This is for public token creation
HTTP_HEADERS: dict[str, str] = {"Content-Type": "application/json"}

# --- Accumulator for all item data from the loop ---
all_item_responses_data_list: list[dict[str, Any]] = []

for institution_id in INSTITUTION_IDS_TO_PROCESS:
    logger.info(msg=f"--- Processing Institution ID: {institution_id} ---")
    ACCESS_TOKEN: Optional[str] = None  # Reset for each institution
    # Call the new function to get the access token
    ACCESS_TOKEN: Optional[str] = get_plaid_access_token(
        institution_id=institution_id,
        client_id=CLIENT_ID,  # Should not be None here due to earlier checks
        secret=SECRET,  # Should not be None here
        initial_products=INITIAL_PRODUCTS,
        http_headers=HTTP_HEADERS,
        override_username=OVERRIDE_USERNAME,
        override_password=OVERRIDE_PASSWORD,
        config=cfgs,  # Pass your main config dictionary
        logger=logger,
        fetch_response_func=fetch_response,  # Pass your actual fetch_response function
    )

    if not ACCESS_TOKEN:
        logger.error(
            msg=f"Could not obtain access token for {institution_id}. Skipping further processing for this institution."
        )
        continue  # Move to the next institution_id

    # Make the POST Request to get Item Data (or Transactions, etc.)
    logger.info(
        msg=f"Access token for {institution_id} is ready. Proceeding to fetch item data..."
    )
    wait_time: int = cfgs.get("WAIT_TIME", 5)
    logger.info(
        msg=f"Waiting for {wait_time} seconds before fetching item data for {institution_id}..."
    )
    time.sleep(wait_time)

    item_payload: dict[str, Any] = {
        "client_id": CLIENT_ID,
        "secret": SECRET,
        "access_token": ACCESS_TOKEN,
    }
    item_response_obj: requests.Response | None = fetch_response(
        api_url=cfgs["BASE_URL"] + cfgs["ITEMS_URL"],  # Assuming ITEMS_URL is /item/get
        headers=HTTP_HEADERS,
        payload=item_payload,
        logger=logger,
    )

    if item_response_obj and item_response_obj.status_code == 200:
        try:
            item_data_for_institution: dict[str, Any] = item_response_obj.json()
            # The response from /item/get typically has 'item' and 'status' keys.
            # We want to append the whole response dictionary for this institution.
            all_item_responses_data_list.append(item_data_for_institution)
            logger.info(
                msg=f"Successfully fetched item data for {institution_id}. Keys: {list(item_data_for_institution.keys())}"
            )
            # logger.debug(f"Item data for {institution_id}: {json.dumps(item_data_for_institution, indent=2)}")
            # logger.info(msg=f"API Response Content: {item_response_obj.text}")
        except json.JSONDecodeError:
            logger.error(
                msg=f"Failed to decode item data JSON for {institution_id}. Raw: {item_response_obj.text[:200]}"
            )
    else:
        logger.error(
            msg=f"Failed to fetch item data for {institution_id} or received non-200 status."
        )

# --- After the loop, save all accumulated data ---
logger.info(
    msg=f"--- All {len(INSTITUTION_IDS_TO_PROCESS)} institutions processed. Now saving data. ---"
)

if not all_item_responses_data_list:
    logger.warning(
        msg="No item data was accumulated from any institution. Output files will be empty or not created."
    )
else:
    # Save all collected item data (list of responses) to a single JSON file
    if JSON_FILE_PATH:
        save_all_item_data_to_json(
            all_item_data_list=all_item_responses_data_list,
            json_filepath=JSON_FILE_PATH,
            logger=logger,
        )
    else:
        logger.warning(
            msg="JSON_FILE_PATH not configured. Consolidated item data not saved to JSON."
        )

    # Save available products from all items to a single CSV file
    if CSV_FILE_PATH:
        save_available_products_to_csv(
            all_item_data_list=all_item_responses_data_list,
            csv_filepath=CSV_FILE_PATH,
            logger=logger,
        )
    else:
        logger.warning(
            msg="CSV_FILE_PATH not configured. Available products not saved to CSV."
        )

logger.info(msg="=== `src/sandbox_plaid/get_item.py` script finished ===")
