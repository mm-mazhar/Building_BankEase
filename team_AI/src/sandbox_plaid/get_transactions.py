# -*- coding: utf-8 -*-
# """
# src/sandbox_plaid/get_transactions.py
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

# --- Path Setup ---
PROJECT_ROOT: str = os.path.abspath(
    path=os.path.join(os.path.dirname(p=__file__), os.pardir, os.pardir)
)
sys.path.insert(0, PROJECT_ROOT)

# --- Imports ---
from configs.configs import cfgs_plaid_sbx
from configs.logging_setup import get_logger
from utils.data_to_json import data_to_json  # Now takes (response_obj, logger)
from utils.delete_files import delete_files_if_exist
from utils.fetch_response import fetch_response
from utils.get_cols_series import get_col_series_from_csv
from utils.json_to_csv import convert_json_to_csv
from utils.get_access_token import get_plaid_access_token

# --- Logger ---
logger: logging.Logger = get_logger()
logger.info(msg="=== Running `src/sandbox_plaid/get_transactions.py`===")

# --- Configuration and Environment Variables ---
cfgs: dict[str, Any] = cfgs_plaid_sbx

ENV_FILE_PATH: Optional[str] = cfgs.get("ENV_FILE_PATH")

load_dotenv(dotenv_path=ENV_FILE_PATH)

CLIENT_ID: Optional[str] = os.getenv(key="PLAID_CLIENT_ID")
SECRET: Optional[str] = os.getenv(key="PLAID_CLIENT_SECRET")
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
    os.path.join(DATA_DIR, "transactions.json") if DATA_DIR else None
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

# --- Plaid API Parameters ---
# INSTITUTION_IDS should be a list in your config or defined here
# INSTITUTION_IDS_TO_PROCESS: List[str] = cfgs.get(
#     "INSTITUTION_IDS", ["ins_137832", "ins_21"]  # ins_20
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
    # If you need to store other parts of the response per institution, add more lists

    if CLIENT_ID is None or SECRET is None:
        logger.error(msg="CLIENT_ID or SECRET is not set. Cannot proceed.")
        return  # or raise an exception, or exit the function

    for institution_id in INSTITUTION_IDS_TO_PROCESS:
        logger.info(msg=f"--- Processing Institution: {institution_id} ---")
        ACCESS_TOKEN: Optional[str] = None  # Reset for each institution

        # Call the new function to get the access token
        ACCESS_TOKEN: Optional[str] = get_plaid_access_token(
            institution_id=institution_id,
            client_id=CLIENT_ID,
            secret=SECRET,
            initial_products=INITIAL_PRODUCTS,
            http_headers=HTTP_HEADERS,
            override_username=OVERRIDE_USERNAME,
            override_password=OVERRIDE_PASSWORD,
            config=cfgs,
            logger=logger,
            fetch_response_func=fetch_response,
        )

        if not ACCESS_TOKEN:
            logger.error(
                msg=f"Could not obtain access token for {institution_id}. Skipping further processing for this institution."
            )
            continue  # Move to the next institution_id

        # Make the POST Request to getTransactions
        logger.info(
            msg=f"Access token for {institution_id} is ready. Proceeding to fetch item data..."
        )

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
                "count": cfgs["TRANSACTIONS_COUNT"],
                "offset": cfgs["TRANSACTIONS_OFFSET"],
            },
        }
        logger.info(msg=f"Fetching transactions for {institution_id}...")
        transactions_response_obj: requests.Response | None = fetch_response(
            api_url=cfgs["BASE_URL"] + cfgs["TRANSACTIONS_URL"],
            headers=HTTP_HEADERS,
            payload=transactions_payload,
            logger=logger,
        )

        # Get Key from Response
        if transactions_response_obj is not None:
            logger.info(
                msg=f"Keys in API Response: {list(transactions_response_obj.json().keys())}"
            )

        # Parse using the MODIFIED response_to_json (which doesn't write to file)
        institution_data: dict[str, Any] | None = data_to_json(
            response_object=transactions_response_obj, logger=logger
        )

        if institution_data:
            accounts: list[dict[str, Any]] = institution_data.get("accounts", [])
            transactions: list[dict[str, Any]] = institution_data.get(
                "transactions", []
            )

            # Add institution_id to each account and transaction for better tracking in combined data
            for acc in accounts:
                acc["institution_id_source"] = institution_id
            for trx in transactions:
                trx["institution_id_source"] = institution_id

            all_accounts_accumulator.extend(accounts)
            all_transactions_accumulator.extend(transactions)
            logger.info(
                msg=f"Accumulated {len(accounts)} accounts and {len(transactions)} transactions for {institution_id}."
            )
        else:
            logger.error(msg=f"No transaction data processed for {institution_id}.")

    # --- After the loop, save all accumulated data ---
    logger.info(
        msg="--- All institutions processed. Consolidating and saving data. ---"
    )

    if not all_accounts_accumulator and not all_transactions_accumulator:
        logger.warning(
            msg="No data accumulated from any institution. Output files might be empty or not created."
        )
        # Create empty JSON if path is specified
        if JSON_FILE_PATH:
            with open(file=JSON_FILE_PATH, mode="w", encoding="utf-8") as f_json_empty:
                json.dump(
                    obj={"accounts": [], "transactions": []}, fp=f_json_empty, indent=2
                )
            logger.info(msg=f"Wrote empty structure to {JSON_FILE_PATH}")
            # Attempt to create empty CSV from the empty JSON
            if CSV_FILE_PATH:
                convert_json_to_csv(
                    json_filepath=JSON_FILE_PATH,
                    csv_filepath=CSV_FILE_PATH,
                    key=INITIAL_PRODUCTS[0],  # or "accounts" based on your need
                    logger=logger,  # Will process the empty accounts list
                )
        return

    final_consolidated_data: dict[str, Any] = {
        "accounts": all_accounts_accumulator,
        "transactions": all_transactions_accumulator,
        "metadata": {  # Example metadata
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
                    obj=final_consolidated_data,
                    fp=f_json_out,
                    ensure_ascii=False,
                    indent=2,
                )
            logger.info(msg=f"Successfully saved consolidated data to {JSON_FILE_PATH}")

            # Now, convert the consolidated JSON to CSV
            if CSV_FILE_PATH:
                logger.info(
                    msg=f"Converting 'accounts' from consolidated JSON to CSV: {CSV_FILE_PATH}"
                )
                csv_success_accounts: bool = convert_json_to_csv(
                    json_filepath=JSON_FILE_PATH,
                    csv_filepath=CSV_FILE_PATH,  # This CSV will contain all accounts
                    key=INITIAL_PRODUCTS[0],  # or "accounts" based on your need
                    logger=logger,
                )
                if csv_success_accounts:
                    logger.info(
                        msg=f"Successfully created CSV for accounts: {CSV_FILE_PATH}"
                    )
                else:
                    logger.error(
                        msg=f"Failed to create CSV for accounts from {JSON_FILE_PATH}"
                    )

                # If you want a separate CSV for transactions:
                # TRANSACTIONS_ONLY_CSV_PATH = CSV_FILE_PATH.replace(".csv", "_transactions.csv")
                # logger.info(f"Converting 'transactions' from consolidated JSON to CSV: {TRANSACTIONS_ONLY_CSV_PATH}")
                # csv_success_transactions = convert_json_to_csv(
                #     json_filepath=JSON_FILE_PATH,
                #     csv_filepath=TRANSACTIONS_ONLY_CSV_PATH,
                #     key=INITIAL_PRODUCTS[0],
                #     logger=logger
                # )
                # if csv_success_transactions:
                #    logger.info(f"Successfully created CSV for transactions: {TRANSACTIONS_ONLY_CSV_PATH}")
                # else:
                #    logger.error(f"Failed to create CSV for transactions from {JSON_FILE_PATH}")
            else:
                logger.warning(
                    msg="CSV_FILE_PATH not configured. Skipping CSV conversion."
                )
        except IOError as e:
            logger.error(
                msg=f"IOError writing consolidated JSON to {JSON_FILE_PATH}: {e}"
            )
        except Exception as e:
            logger.error(
                msg=f"Unexpected error saving consolidated JSON: {e}", exc_info=True
            )
    else:
        logger.warning(
            msg="JSON_FILE_PATH not configured. Consolidated data not saved."
        )

    logger.info(msg=f"=== Product: '{INITIAL_PRODUCTS[0]}`s' Workflow Completed ===")


if __name__ == "__main__":
    if not all(
        [CLIENT_ID, SECRET, cfgs, JSON_FILE_PATH]
    ):  # CSV_FILE_PATH is optional for just JSON
        logger.critical(
            msg="One or more critical configurations (CLIENT_ID, SECRET, cfgs, JSON_FILE_PATH) are missing. Aborting."
        )
        sys.exit(1)
    run_workflow()
