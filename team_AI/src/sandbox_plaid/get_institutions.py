# -*- coding: utf-8 -*-
# """
# src/sandbox_plaid/get_institutions.py
# Created on June 02, 2025
# @ Author: Mazhar
# """

import logging
import os
import sys
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

from utils.delete_files import delete_files_if_exist

# Import Utils Functions
from utils.fetch_response import fetch_response
from utils.json_to_csv import convert_json_to_csv
from utils.response_to_json import response_to_json

logger.info(msg="=== Running `src/sandbox_plaid/get_institutions.py`")
# logger.info(msg=f"Project root: {PROJECT_ROOT}")
# logger.info(msg=f"Python path: {sys.path}")
# logger.info(msg=f"Configuration: {cfgs_plaid_sbx}")

# # --- Configuration ---
ENV_FILE_PATH: str | None = cfgs_plaid_sbx.get("ENV_FILE_PATH")
# logger.info(msg=f"ENV FILE PATH: {ENV_FILE_PATH}")

# --- Load Environment Variables ---
load_dotenv(dotenv_path=ENV_FILE_PATH)
CLIENT_ID: str | None = os.getenv(key="PLAID_CLIENT_ID")
SECRET: str | None = os.getenv(key="PLAID_CLIENT_SECRET")

# Extract values from YAML (use .get() for safety if keys might be missing)
PLAID_URL: str | None = cfgs_plaid_sbx.get("INSTITUTIONS_URL")
COUNT: int | None = cfgs_plaid_sbx.get("INSTITUTIONS_COUNT")
OFFSET: int | None = cfgs_plaid_sbx.get("INSTITUTIONS_OFFSET")
BANK_PRODUCTS: Optional[list[str]] = cfgs_plaid_sbx.get("BANK_PRODUCTS")
COUNTRY_CODES_LIST: Optional[list[str]] = cfgs_plaid_sbx.get("COUNTRY_CODES")

if not all(
    [
        CLIENT_ID,
        SECRET,
        PLAID_URL,
        COUNT,
        OFFSET,
        BANK_PRODUCTS,
        COUNTRY_CODES_LIST,
    ]
):
    logger.critical("Critical configurations missing. Exiting.")
    sys.exit(1)

DATA_DIR: str | None = cfgs_plaid_sbx.get("DATA_DIR")
if DATA_DIR is not None:
    dir_path: str = os.path.dirname(DATA_DIR)
    if not os.path.exists(path=dir_path):
        os.makedirs(name=dir_path)
else:
    logger.info(msg="Error: DATA_DIR missing in YAML CONFIG FILE")
    exit(1)

JSON_FILE_PATH: str | None = (
    os.path.join(DATA_DIR, "institutions.json") if DATA_DIR else None
)
CSV_FILE_PATH: str | None = (
    os.path.join(DATA_DIR, "institutions.csv") if DATA_DIR else None
)
logger.info(msg=f"JSON_FILE_PATH: {JSON_FILE_PATH}")
logger.info(msg=f"CSV_FILE_PATH: {CSV_FILE_PATH}")

# Delete JSON_FILE_PATH and CSV_FILE_PATH if they exist to ensure fresh data
files_to_delete: list[Optional[str]] = [
    JSON_FILE_PATH,
    CSV_FILE_PATH,
    None,
]
delete_files_if_exist(file_paths=files_to_delete, logger=logger)

# --- Prepare Request ---
headers: dict[str, str] = {"Content-Type": "application/json"}

payload: dict[str, Any] = {
    "client_id": CLIENT_ID,
    "secret": SECRET,
    "count": COUNT,  # This should be an integer
    "offset": OFFSET,  # This should be an integer
    "country_codes": COUNTRY_CODES_LIST,  # This should be a list of strings
}

# --- Make the POST Request ---
response: Optional[requests.Response] = None  # Initialize to None
parsed_data: dict[str, Any] | None = None  # Initialize parsed_data

api_response: requests.Response | None = fetch_response(
    api_url=cfgs_plaid_sbx["BASE_URL"] + PLAID_URL,
    headers=headers,
    payload=payload,
    logger=logger,
)

# Get Key from Response
if api_response is not None:
    logger.info(msg=f"Keys in API Response: {list(api_response.json().keys())}")

# --- Save Response to .JSON File ---
if api_response and JSON_FILE_PATH is not None:
    logger.info(msg="--- Saving response to JSON file---")
    parsed_data: dict[str, Any] | None = response_to_json(
        response_object=api_response, logger=logger, json_file_path=JSON_FILE_PATH
    )
    if parsed_data:
        logger.info(
            msg=f"Successfully processed and saved data. Keys: {list(parsed_data.keys())}"
        )
    else:
        logger.error(msg="Failed to process or save the response data as JSON.")
else:
    logger.error(msg="Failed to fetch data from API.")

# --- Save Response to .CSV File ---
if (
    (parsed_data is not None)
    and (JSON_FILE_PATH is not None)
    and (CSV_FILE_PATH is not None)
):
    logger.info(msg="--- Converting JSON to CSV ---")
    convert_json_to_csv(
        json_filepath=JSON_FILE_PATH,
        csv_filepath=CSV_FILE_PATH,
        logger=logger,
        key="institutions",
    )
else:
    logger.error(
        msg="parsed_data or JSON_FILE_PATH or CSV_FILE_PATH is None, cannot convert JSON to CSV."
    )
