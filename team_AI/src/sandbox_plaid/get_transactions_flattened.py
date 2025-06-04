# -*- coding: utf-8 -*-
# """
# src/sandbox_plaid/get_transactions_flattened.py
# Details: MODIFIED Version of `get_transactions.py` for comprehensive CSV output
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

# --- Path Setup --- (Same as before)
PROJECT_ROOT: str = os.path.abspath(
    path=os.path.join(os.path.dirname(p=__file__), os.pardir, os.pardir)
)
sys.path.insert(0, PROJECT_ROOT)

# --- Imports ---
from configs.configs import cfgs_plaid_sbx
from configs.logging_setup import get_logger
from utils.delete_files import delete_files_if_exist
from utils.fetch_response import fetch_response
from utils.flattened_data import flatten_plaid_transactions_data
from utils.get_cols_series import get_col_series_from_csv

# Import the NEW CSV saving function
from utils.list_of_dicts_to_csv import save_list_of_dicts_to_csv
from utils.response_to_json import response_to_json

# ... (Logger setup, Config loading, ENV vars, DATA_DIR, File Paths - same as your previous version) ...
logger: logging.Logger = get_logger()
logger.info(
    msg="=== Running `src/sandbox_plaid/get_transactions_flattened.py` (Comprehensive CSV) ==="
)

cfgs: dict[str, Any] = cfgs_plaid_sbx

ENV_FILE_PATH: Optional[str] = cfgs.get("ENV_FILE_PATH")
load_dotenv(dotenv_path=ENV_FILE_PATH)

CLIENT_ID: Optional[str] = os.getenv("PLAID_CLIENT_ID")
SECRET: Optional[str] = os.getenv("PLAID_CLIENT_SECRET")
OVERRIDE_USERNAME: Optional[str] = os.getenv("PLAID_LINK_USERNAME", "user_good")
OVERRIDE_PASSWORD: Optional[str] = os.getenv("PLAID_LINK_PASSWORD", "pass_good")

DATA_DIR: Optional[str] = cfgs.get("DATA_DIR")
if DATA_DIR:
    os.makedirs(name=DATA_DIR, exist_ok=True)
else:
    logger.critical(msg="DATA_DIR missing in configuration. Exiting.")
    sys.exit(1)

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

JSON_FILE_PATH: str | None = (
    os.path.join(DATA_DIR, "transactions_bal.json") if DATA_DIR else None
)
CSV_FILE_PATH: str | None = (
    os.path.join(DATA_DIR, "transactions.csv") if DATA_DIR else None
)
INSTITUTION_FILE_PATH: Optional[str] = (
    os.path.join(DATA_DIR, "institutions.csv") if DATA_DIR else None
)

logger.info(msg=f"JSON_FILE_PATH: {JSON_FILE_PATH}")
logger.info(msg=f"CSV_FILE_PATH: {CSV_FILE_PATH}")
logger.info(msg=f"Institution CSV file path: {INSTITUTION_FILE_PATH}")

# Delete JSON_FILE_PATH and CSV_FILE_PATH if they exist to ensure fresh data
files_to_delete: list[Optional[str]] = [
    JSON_FILE_PATH,
    CSV_FILE_PATH,
    None,
]
delete_files_if_exist(file_paths=files_to_delete, logger=logger)

# INSTITUTION_IDS_TO_PROCESS: list[str] = cfgs.get(
#     "INSTITUTION_IDS", ["ins_20", "ins_21"]
# )
if INSTITUTION_FILE_PATH:
    INSTITUTION_IDS_TO_PROCESS: list[str] = get_col_series_from_csv(
        csv_filepath=INSTITUTION_FILE_PATH, column_name="institution_id"
    )
    logger.info(msg=f"Institution IDs: {INSTITUTION_IDS_TO_PROCESS}")

INITIAL_PRODUCTS: list[str] = ["transactions"]
HTTP_HEADERS: dict[str, str] = {"Content-Type": "application/json"}


def run_workflow() -> None:
    all_accounts_accumulator: list[dict[str, Any]] = []
    all_transactions_accumulator: list[dict[str, Any]] = []
    # If each institution fetch returns a full response including 'item', you might want to store them
    all_item_objects_accumulator: list[dict[str, Any]] = (
        []
    )  # Or one if it's always the same item

    for institution_id in INSTITUTION_IDS_TO_PROCESS:
        logger.info(msg=f"--- Processing Institution: {institution_id} ---")
        ACCESS_TOKEN: Optional[str] = None

        # ... (Steps 1 & 2: Public Token and Access Token - same as your previous correct version)
        # Step 1: Create Public Token
        pt_payload: dict[str, Any] = {
            "client_id": CLIENT_ID,
            "secret": SECRET,
            "institution_id": institution_id,
            "initial_products": INITIAL_PRODUCTS,
            "options": {
                "webhook": cfgs.get("WEBHOOK_URL", "https://www.example.com/webhook"),
                "override_username": OVERRIDE_USERNAME,
                "override_password": OVERRIDE_PASSWORD,
            },
        }
        pt_response_obj: requests.Response | None = fetch_response(
            api_url=cfgs["BASE_URL"] + cfgs["CREATE_PUBLIC_TOKEN_URL"],
            headers=HTTP_HEADERS,
            payload=pt_payload,
            logger=logger,
        )
        PUBLIC_TOKEN: Optional[str] = None
        if pt_response_obj and pt_response_obj.status_code == 200:
            try:
                pt_data: dict[str, Any] = pt_response_obj.json()
                PUBLIC_TOKEN = pt_data.get("public_token")
            except json.JSONDecodeError:
                logger.error(
                    msg=f"Failed to decode JSON from public token response for {institution_id}. Raw: {pt_response_obj.text[:200]}"
                )
        if not PUBLIC_TOKEN:
            logger.error(
                msg=f"Failed to obtain public token for {institution_id}. Skipping."
            )
            continue
        logger.info(msg=f"Public token obtained for {institution_id}.")

        # Step 2: Exchange Public Token for Access Token
        exchange_payload: dict[str, Any] = {
            "client_id": CLIENT_ID,
            "secret": SECRET,
            "public_token": PUBLIC_TOKEN,
        }
        exchange_response_obj: requests.Response | None = fetch_response(
            api_url=cfgs["BASE_URL"] + cfgs["EXCHANGE_TOKEN_URL"],
            headers=HTTP_HEADERS,
            payload=exchange_payload,
            logger=logger,
        )
        if exchange_response_obj and exchange_response_obj.status_code == 200:
            try:
                exchange_data: dict[str, Any] = exchange_response_obj.json()
                ACCESS_TOKEN = exchange_data.get("access_token")
            except json.JSONDecodeError:
                logger.error(
                    msg=f"Failed to decode JSON from token exchange response for {institution_id}. Raw: {exchange_response_obj.text[:200]}"
                )
        if not ACCESS_TOKEN:
            logger.error(
                msg=f"Failed to obtain access token for {institution_id}. Skipping."
            )
            continue
        logger.info(msg=f"Access token obtained for {institution_id}.")

        # Step 3: Get Transactions
        wait_time: int = cfgs.get("WAIT_TIME", 5)
        logger.info(
            msg=f"Waiting for {wait_time} seconds before fetching transactions for {institution_id}..."
        )
        time.sleep(wait_time)
        transactions_payload: dict[str, Any] = {
            "client_id": CLIENT_ID,
            "secret": SECRET,
            "access_token": ACCESS_TOKEN,
            "start_date": cfgs["TRANSACTIONS_START_DATE"],
            "end_date": cfgs["TRANSACTIONS_END_DATE"],
            "options": {
                "count": cfgs.get("TRANSACTIONS_COUNT", 100),
                "offset": cfgs.get("TRANSACTIONS_OFFSET", 0),
            },
        }
        transactions_response_obj: requests.Response | None = fetch_response(
            api_url=cfgs["BASE_URL"] + cfgs["TRANSACTIONS_URL"],
            headers=HTTP_HEADERS,
            payload=transactions_payload,
            logger=logger,
        )

        # Parse using the MODIFIED response_to_json (which doesn't write to file)
        if DATA_DIR is not None:
            temp_json_path = os.path.join(DATA_DIR, "temp.json")
        else:
            logger.error("DATA_DIR is None, cannot create temp.json path.")
            temp_json_path = "temp.json"  # fallback or handle as needed

        full_institution_response_data: dict[str, Any] | None = response_to_json(
            response_object=transactions_response_obj,
            logger=logger,
            json_file_path=temp_json_path,
        )

        if full_institution_response_data:
            accounts: list[dict[str, Any]] = full_institution_response_data.get(
                "accounts", []
            )
            transactions: list[dict[str, Any]] = full_institution_response_data.get(
                "transactions", []
            )
            item_details: dict[str, Any] = full_institution_response_data.get(
                "item", {}
            )  # Get the item object

            for acc in accounts:
                acc["institution_id_source"] = institution_id
            for trx in transactions:
                trx["institution_id_source"] = institution_id  # Add source inst ID

            all_accounts_accumulator.extend(accounts)
            all_transactions_accumulator.extend(transactions)
            # If 'item' details are specific per institution run AND you need them later for flattening,
            # you'd store item_details too, perhaps keyed by institution_id or item_id.
            # For now, we'll assume the 'item' from the *last successful call* might be used,
            # or we make it part of the consolidated JSON for the flattening function to use.
            if (
                item_details
            ):  # Store the item details (potentially the last one if looping multiple items)
                all_item_objects_accumulator.append(item_details)

            logger.info(
                msg=f"Accumulated {len(accounts)} accounts and {len(transactions)} transactions for {institution_id}."
            )
        else:
            logger.error(msg=f"No transaction data processed for {institution_id}.")

    logger.info(
        msg="--- All institutions processed. Consolidating and preparing final data. ---"
    )

    if (
        not all_transactions_accumulator
    ):  # Check if any transactions were gathered at all
        logger.warning(
            msg="No transactions accumulated from any institution. Output files will reflect this."
        )
        # Create empty JSON and CSV if paths are specified
        if JSON_FILE_PATH:
            with open(file=JSON_FILE_PATH, mode="w", encoding="utf-8") as f_json_empty:
                json.dump(
                    obj={"accounts": [], "transactions": [], "item": {}},
                    fp=f_json_empty,
                    indent=2,
                )
            logger.info(msg=f"Wrote empty structure to {JSON_FILE_PATH}")
            if CSV_FILE_PATH:
                save_list_of_dicts_to_csv(
                    data_list=[], csv_filepath=CSV_FILE_PATH, logger=logger
                )  # Pass empty list
        return

    # For simplicity in this example, we'll use the item details from the *first successful* fetch
    # In a real multi-item scenario, you'd need to associate item details more carefully.
    # Or, if all transactions belong to effectively the same 'item' context for your report, this is fine.
    final_item_details: dict[str, Any] = (
        all_item_objects_accumulator[0] if all_item_objects_accumulator else {}
    )

    final_consolidated_data_for_json: dict[str, Any] = {
        "accounts": all_accounts_accumulator,
        "transactions": all_transactions_accumulator,
        "item": final_item_details,  # Add item details here
        "metadata": {
            "processed_institution_ids": INSTITUTION_IDS_TO_PROCESS,
            "total_accounts_retrieved": len(all_accounts_accumulator),
            "total_transactions_retrieved": len(all_transactions_accumulator),
        },
    }

    if JSON_FILE_PATH:
        logger.info(msg=f"Saving consolidated data to JSON: {JSON_FILE_PATH}")
        try:
            with open(file=JSON_FILE_PATH, mode="w", encoding="utf-8") as f_json_out:
                json.dump(
                    obj=final_consolidated_data_for_json,
                    fp=f_json_out,
                    ensure_ascii=False,
                    indent=2,
                )
            logger.info(msg=f"Successfully saved consolidated data to {JSON_FILE_PATH}")

            if CSV_FILE_PATH:
                logger.info(
                    msg=f"Preparing and saving comprehensive CSV to: {CSV_FILE_PATH}"
                )
                # Use the new flatten_plaid_transactions_data function
                flat_data_for_csv: list[dict[str, Any]] = (
                    flatten_plaid_transactions_data(
                        full_data=final_consolidated_data_for_json,  # Pass the whole structure
                        logger_instance=logger,
                    )
                )

                csv_success: bool = save_list_of_dicts_to_csv(
                    data_list=flat_data_for_csv,
                    csv_filepath=CSV_FILE_PATH,
                    logger=logger,
                )
                if csv_success:
                    logger.info(
                        msg=f"Successfully created comprehensive CSV: {CSV_FILE_PATH}"
                    )
                else:
                    logger.error(msg=f"Failed to create comprehensive CSV.")
            else:
                logger.warning(
                    msg="CSV_FILE_PATH not configured. Skipping CSV generation."
                )
        except Exception as e:  # Catch broader errors during JSON save or CSV prep
            logger.error(
                msg=f"Error during final JSON saving or CSV preparation: {e}",
                exc_info=True,
            )
    else:
        logger.warning(
            msg="JSON_FILE_PATH not configured. Consolidated data not saved."
        )

    logger.info(msg="=== Plaid Transaction Workflow Completed ===")


if __name__ == "__main__":
    if not all([CLIENT_ID, SECRET, cfgs, JSON_FILE_PATH]):
        logger.critical("Critical configurations missing. Aborting.")
        sys.exit(1)
    run_workflow()
