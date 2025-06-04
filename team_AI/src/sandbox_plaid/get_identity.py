# -*- coding: utf-8 -*-
# """
# src/sandbox_plaid/get_identity.py
# Created on June 04, 2025
# @ Author: Mazhar
# """


import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional

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
from utils.flattened_data import flatten_identity_data_to_list_of_dicts
from utils.list_of_dicts_to_csv import save_list_of_dicts_to_csv

logger.info(msg="=== Running `src/sandbox_plaid/get_identity.py`")
# logger.info(msg=f"Project root: {PROJECT_ROOT}")
# logger.info(msg=f"Python path: {sys.path}")
# logger.info(msg=f"Configuration: {cfgs_plaid_sbx}")

# --- Configuration and Environment Variables ---
cfgs: dict[str, Any] = cfgs_plaid_sbx

if not cfgs:
    logger.error(
        msg="Configuration not found. Please check your .env file or configuration settings."
    )
    exit(1)

ENV_FILE_PATH: str | None = cfgs["ENV_FILE_PATH"]
# logger.info(msg=f"ENV FILE PATH: {ENV_FILE_PATH}")
load_dotenv(dotenv_path=ENV_FILE_PATH)
CLIENT_ID: str | None = os.getenv(key="PLAID_CLIENT_ID")
SECRET: str | None = os.getenv(key="PLAID_CLIENT_SECRET")
OVERRIDE_USERNAME: str | None = os.getenv(key="PLAID_LINK_USERNAME")
OVERRIDE_PASSWORD: str | None = os.getenv(key="PLAID_LINK_PASSWORD")
# logger.info(msg=f"PLAID CLIENT ID: {client_id}")
# logger.info(msg=f"PLAID CLIENT SECRET: {secret}")
if not CLIENT_ID or not SECRET:
    logger.info(
        msg=f"Error: PLAID_CLIENT_ID or PLAID_CLIENT_SECRET not found in {ENV_FILE_PATH}"
    )
    logger.info(msg="Please ensure they are set in your .env file.")
    exit(1)

INITIAL_PRODUCTS: list[str] = ["identity"]  # This is for public token creation
HTTP_HEADERS: dict[str, str] = {"Content-Type": "application/json"}

DATA_DIR: str | None = cfgs["DATA_DIR"]
if DATA_DIR is not None:
    dir_path: str = os.path.dirname(DATA_DIR)
    if not os.path.exists(path=dir_path):
        os.makedirs(name=dir_path)
else:
    logger.info(msg="Error: DATA_DIR missing in YAML CONFIG FILE")
    exit(1)

INSTITUTIONS_CSV_FILENAME: Optional[str] = cfgs.get("INSTITUTIONS_CSV")
INSTITUTION_FILE_PATH: Optional[str] = (
    os.path.join(DATA_DIR, INSTITUTIONS_CSV_FILENAME)
    if INSTITUTIONS_CSV_FILENAME
    else None
)
CSV_FILE_PATH: str | None = (
    os.path.join(DATA_DIR, INITIAL_PRODUCTS[0] + ".csv")
    if DATA_DIR and INITIAL_PRODUCTS
    else None
)
logger.info(msg=f"Institution CSV file path: {INSTITUTION_FILE_PATH}")
logger.info(msg=f"CSV_FILE_PATH: {CSV_FILE_PATH}")

# Delete JSON_FILE_PATH and CSV_FILE_PATH if they exist to ensure fresh data
files_to_delete: list[Optional[str]] = [CSV_FILE_PATH]
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

    # Make the POST Request to get Accounts
    logger.info(
        msg=f"Access token for {institution_id} is ready. Proceeding to fetch account data..."
    )
    wait_time: int = cfgs.get("WAIT_TIME", 5)
    logger.info(
        msg=f"Waiting for {wait_time} seconds before fetching account data for {institution_id}..."
    )
    time.sleep(wait_time)

    payload: dict[str, Any] = {
        "client_id": CLIENT_ID,
        "secret": SECRET,
        "access_token": ACCESS_TOKEN,
    }
    response_obj: requests.Response | None = fetch_response(
        api_url=cfgs["BASE_URL"] + cfgs["IDENTITY_URL"],
        headers=HTTP_HEADERS,
        payload=payload,
        logger=logger,
    )

    if response_obj and response_obj.status_code == 200:
        try:
            identity_data_for_institution: dict[str, Any] = response_obj.json()
            all_item_responses_data_list.append(identity_data_for_institution)
            logger.info(
                msg=f"Successfully fetched identity data for {institution_id}. Keys: {list(identity_data_for_institution.keys())}"
            )
            # logger.debug(f"Identity data for {institution_id}: {json.dumps(identity_data_for_institution, indent=2)}")
            # logger.info(msg=f"API Response Content: {response_obj.text}")
        except json.JSONDecodeError:
            logger.error(
                msg=f"Failed to decode identity data JSON for {institution_id}. Raw: {response_obj.text[:200]}"
            )
    else:
        logger.error(
            msg=f"Failed to fetch identity data for {institution_id} or received non-200 status."
        )

# --- After all loops, process accumulated data ---
logger.info(
    msg=f"--- All {len(INSTITUTION_IDS_TO_PROCESS)} institutions processed. Now processing and saving data. ---"
)

if not all_item_responses_data_list:
    logger.warning(
        msg="No data was accumulated. Output CSV will be empty or not created."
    )
else:
    # Optionally, save all raw responses to one JSON file for debugging/archive
    # if RAW_RESPONSES_JSON_PATH:
    #     save_all_item_data_to_json( # Reusing this as it saves a list of dicts
    #         all_item_data_list=all_product_api_responses,
    #         json_filepath=RAW_RESPONSES_JSON_PATH,
    #         logger=logger
    #     )

    # Flatten the accumulated identity data and save to CSV
    if CSV_FILE_PATH:
        logger.info(msg="Flattening identity data for CSV output...")
        flat_identity_list: List[Dict[str, Any]] = (
            flatten_identity_data_to_list_of_dicts(
                all_raw_responses=all_item_responses_data_list,  # Pass all collected responses
                logger=logger,
            )
        )

        if flat_identity_list:
            save_success: bool = save_list_of_dicts_to_csv(
                data_list=flat_identity_list,
                csv_filepath=CSV_FILE_PATH,
                logger=logger,
            )
            if save_success:
                logger.info(
                    msg=f"Successfully saved flattened identity data to {CSV_FILE_PATH}"
                )
            else:
                logger.error(
                    msg=f"Failed to save flattened identity data to {CSV_FILE_PATH}"
                )
        else:
            logger.info(
                msg="No data to save to identity CSV after flattening (list was empty)."
            )
            # Optionally create an empty CSV
            save_list_of_dicts_to_csv(
                data_list=[], csv_filepath=CSV_FILE_PATH, logger=logger
            )

    else:
        logger.warning(
            "CSV_FILE_PATH not configured. Flattened identity data not saved."
        )

logger.info(msg="=== `src/sandbox_plaid/get_data.py` script finished ===")
