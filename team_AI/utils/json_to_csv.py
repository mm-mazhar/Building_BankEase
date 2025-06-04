# -*- coding: utf-8 -*-
# """
# json_to_csv.py
# Created on June 02, 2025
# @ Author: Mazhar
# """

import logging
import os
from typing import Any, Dict, List

# Import the logger setup
from configs.logging_setup import get_logger

# Get the logger
logger: logging.Logger = get_logger()

import json

import pandas as pd


def convert_json_to_csv(
    json_filepath: str,
    csv_filepath: str,
    logger: logging.Logger = logger,
    key: str = "institutions",
) -> bool:
    """
    Reads a JSON file containing Plaid 'key' data, converts the
    'key' list to a Pandas DataFrame, and saves it as a CSV.

    Args:
        json_filepath (str): Path to the input JSON file.
        csv_filepath (str): Path to the output CSV file.

    Returns:
        bool: True if conversion was successful, False otherwise.
    """
    logger.info(msg=f"Converting: {json_filepath} -> {csv_filepath}")

    if not os.path.exists(path=json_filepath):
        logger.error(msg=f"Input JSON file not found: {json_filepath}")
        return False

    try:
        with open(file=json_filepath, mode="r", encoding="utf-8") as f:
            data: dict[str, Any] = json.load(fp=f)
        logger.info(msg=f"Successfully loaded JSON data from: {json_filepath}")

    except json.JSONDecodeError as e:
        logger.error(msg=f"Error decoding JSON from {json_filepath}: {e}")
        return False
    except IOError as e:
        logger.error(msg=f"Error reading JSON file {json_filepath}: {e}")
        return False

    if key not in data or not isinstance(data[key], list):
        logger.error(
            msg=f"'{key}' key not found in JSON or it's not a list. "
            "Cannot create DataFrame."
        )
        return False

    data_list: list[dict[str, Any]] = data[key]

    if not data_list:
        logger.info(msg=f"The '{key}' list is empty. No data to convert to CSV.")
        # Optionally create an empty CSV with headers or just skip
        # For now, we'll consider this a successful (though empty) conversion
        try:
            # If you want an empty CSV with headers, you'd need to infer headers
            # from a sample or have them predefined. For an empty list, this is tricky.
            # Let's just create an empty file or skip.
            # For simplicity, we can just return True as there's no data to write.
            # df_empty = pd.DataFrame(columns=['institution_id', 'name', ...]) # if you have known headers
            # df_empty.to_csv(csv_filepath, index=False, encoding='utf-8')
            logger.info(
                msg=f"Empty {key} list. CSV file '{csv_filepath}' will not contain data rows."
            )
            # To ensure a file is created, even if empty, one might do:
            pd.DataFrame(data=data_list).to_csv(
                path_or_buf=csv_filepath, index=False, mode="w", encoding="utf-8"
            )

        except Exception as e:
            logger.error(msg=f"Error creating an empty/header CSV {csv_filepath}: {e}")
            return False
        return True

    try:
        # Create DataFrame directly from the list of institution dictionaries
        df = pd.DataFrame(data=data_list)
        logger.info(
            msg=f"Successfully created DataFrame with {len(df)} rows and {len(df.columns)} columns."
        )
        logger.debug(msg=f"DataFrame columns: {df.columns.tolist()}")
        logger.debug(msg=f"DataFrame head:\n{df.head()}")

        # Pandas will represent list columns (like 'country_codes', 'products')
        # as Python list objects in the DataFrame cells. When saving to CSV,
        # these will be converted to their string representation (e.g., "['US', 'GB']").
        # If you want them ;-separated like in the previous script, you'd transform them:
        # for col in ['country_codes', 'products', 'routing_numbers']:
        #     if col in df.columns:
        #         df[col] = df[col].apply(lambda x: ';'.join(map(str, x)) if isinstance(x, list) else x)

        df.to_csv(path_or_buf=csv_filepath, index=False, encoding="utf-8")
        logger.info(msg=f"Successfully saved DataFrame to CSV: {csv_filepath}")
        return True

    except Exception as e:
        logger.error(
            msg=f"An error occurred during DataFrame creation or CSV writing: {e}"
        )
        return False


def save_available_products_to_csv(
    all_item_data_list: list[dict[str, Any]], csv_filepath: str, logger: logging.Logger
) -> bool:
    """
    Extracts 'institution_id', 'item_id', and 'available_products' from a list of
    item data dictionaries and saves them to a CSV file.
    Each available product will be a new row, associated with its item_id and institution_id.

    Args:
        all_item_data_list (list[dict[str, Any]]): List of item data dictionaries.
        csv_filepath (str): Full path to the output CSV file.
        logger (logging.Logger): Logger instance.

    Returns:
        bool: True if saving was successful, False otherwise.
    """
    if not csv_filepath:
        logger.error(msg="CSV file path not provided for saving available products.")
        return False

    logger.info(msg=f"Extracting available products and saving to CSV: {csv_filepath}")

    records_for_csv: list[dict[str, Any]] = []
    for item_data_from_response in all_item_data_list:
        # The /item/get response has 'item' as a top-level key,
        # and inside that is the actual item details.
        actual_item_details: Any | None = item_data_from_response.get("item")
        if not actual_item_details or not isinstance(actual_item_details, dict):
            logger.warning(
                msg=f"Skipping entry due to missing or invalid 'item' key: {item_data_from_response.get('request_id', 'N/A')}"
            )
            continue

        institution_id: str | None = actual_item_details.get("institution_id")
        item_id: str | None = actual_item_details.get("item_id")
        available_products: list[str] | None = actual_item_details.get(
            "available_products"
        )

        if not institution_id or not item_id:
            logger.warning(
                msg=f"Skipping item due to missing institution_id or item_id. Request ID: {item_data_from_response.get('request_id', 'N/A')}"
            )
            continue

        if available_products and isinstance(available_products, list):
            for product in available_products:
                records_for_csv.append(
                    {
                        "institution_id": institution_id,
                        "item_id": item_id,
                        "available_product": product,
                    }
                )
        elif available_products is None:
            records_for_csv.append(
                {  # Record that no products were available for this item
                    "institution_id": institution_id,
                    "item_id": item_id,
                    "available_product": None,  # Or an empty string, or a specific "NO_PRODUCTS_AVAILABLE"
                }
            )
        else:
            logger.warning(
                msg=f"No 'available_products' list found or invalid format for item_id {item_id} (institution: {institution_id})."
            )

    if not records_for_csv:
        logger.info(
            msg="No available products found to save to CSV. Creating an empty CSV if specified."
        )
        # Create empty CSV with headers
        try:
            df_empty = pd.DataFrame(
                columns=["institution_id", "item_id", "available_product"]
            )
            df_empty.to_csv(
                path_or_buf=csv_filepath, index=False, mode="w", encoding="utf-8"
            )
            logger.info(msg=f"Empty CSV created at {csv_filepath} with headers.")
            return True  # Or False if an empty file is considered a failure for this function
        except Exception as e:
            logger.error(msg=f"Failed to create empty CSV: {e}", exc_info=True)
            return False

    try:
        df = pd.DataFrame(data=records_for_csv)
        df.to_csv(
            path_or_buf=csv_filepath, index=False, mode="w", encoding="utf-8"
        )  # mode='w' to overwrite
        logger.info(msg=f"Successfully saved available products to CSV: {csv_filepath}")
        return True
    except Exception as e:
        logger.error(msg=f"Error writing available products to CSV: {e}", exc_info=True)
    return False
