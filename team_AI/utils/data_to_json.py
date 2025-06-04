# -*- coding: utf-8 -*-
# """
# utils/data_to_json.py
# Created on June 02, 2025
# @ Author: Mazhar
# """

import json
import logging
from typing import Any, Optional
import requests


def data_to_json(
    response_object: Optional[requests.Response],
    logger: logging.Logger,
    # json_file_path: str, <--- REMOVE THIS ARGUMENT
) -> Optional[dict[str, Any]]:
    """
    Processes a requests.Response object and attempts to parse it as JSON.
    Does NOT write to a file.

    Args:
        response_object (Optional[requests.Response]): The Response object from the API call.
        logger (logging.Logger): Logger instance for logging messages.

    Returns:
        Optional[dict[str, Any]]: The parsed JSON data as a dictionary if successful,
                                  otherwise None.
    """
    if response_object is None:
        logger.warning(msg="Received no response object to process for JSON parsing.")
        return None

    try:
        # logger.info("Attempting to parse response as JSON...") # Caller can log this
        response_data: dict[str, Any] = response_object.json()
        # No longer logs the full pretty JSON to console here; caller can do it if needed.
        logger.debug(msg="Successfully parsed response to JSON.")
        return response_data

    except requests.exceptions.JSONDecodeError:
        logger.error(msg="Response was not valid JSON.")
        if response_object and hasattr(response_object, "text"):
            logger.info(
                msg=f"Raw response text for JSONDecodeError: {response_object.text[:500]}..."
            )  # Log snippet
        return None
    except Exception as e:
        logger.error(
            msg=f"An unexpected error occurred during JSON parsing: {e}",
            exc_info=True,
        )
        return None


def save_all_item_data_to_json(
    all_item_data_list: list[dict[str, Any]], json_filepath: str, logger: logging.Logger
) -> bool:
    """
    Saves a list of item data dictionaries to a single JSON file.
    Each dictionary in the list typically represents the 'item' object
    from a Plaid /item/get response for a specific institution.

    Args:
        all_item_data_list (list[dict[str, Any]]): List of item data dictionaries.
        json_filepath (str): Full path to the output JSON file.
        logger (logging.Logger): Logger instance.

    Returns:
        bool: True if saving was successful, False otherwise.
    """
    if not json_filepath:
        logger.error(msg="JSON file path not provided for saving item data.")
        return False

    logger.info(
        msg=f"Attempting to save {len(all_item_data_list)} item data entries to JSON: {json_filepath}"
    )
    try:
        # The top-level structure will be a JSON array of these item objects.
        # If you want a different structure (e.g., a dictionary with a key like "items"),
        # wrap all_item_data_list in that structure before dumping.
        # For this example, we save it as a list of items.
        with open(file=json_filepath, mode="w", encoding="utf-8") as f_json:
            json.dump(obj=all_item_data_list, fp=f_json, ensure_ascii=False, indent=2)
        logger.info(msg=f"Successfully saved all item data to JSON: {json_filepath}")
        return True
    except IOError as e:
        logger.error(
            msg=f"IOError writing item data to JSON file {json_filepath}: {e}",
            exc_info=True,
        )
    except TypeError as e:
        logger.error(
            msg=f"TypeError - item data not JSON serializable for {json_filepath}: {e}",
            exc_info=True,
        )
    return False
