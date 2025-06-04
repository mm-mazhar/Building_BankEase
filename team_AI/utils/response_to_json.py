# -*- coding: utf-8 -*-
# """
# response_to_json.py
# Created on June 02, 2025
# @ Author: Mazhar
# """

import json
import logging
from typing import Any, Optional

import requests


# --- Save Response to JSON ---
def response_to_json(
    response_object: Optional[requests.Response],
    logger: logging.Logger,
    json_file_path: str,  # Made non-optional for this function's core purpose
) -> Optional[dict[str, Any]]:
    """
    Processes a requests.Response object, attempts to parse it as JSON,
    logs details, and saves the JSON data to a file. If parsing fails,
    it saves the raw text.

    Args:
        response_object (Optional[requests.Response]): The Response object from the API call.
        logger (logging.Logger): Logger instance for logging messages.
        json_file_path (str): Path to save the JSON response or raw text on error.

    Returns:
        Optional[dict[str, Any]]: The parsed JSON data as a dictionary if successful,
                                  otherwise None.
    """
    if response_object is None:
        logger.warning(msg="Received no response object to process.")
        return None

    try:
        logger.info(msg="Attempting to parse response as JSON...")
        response_data: dict[str, Any] = response_object.json()
        pretty_json_response_str: str = json.dumps(obj=response_data, indent=2)
        # logger.info(
        #     msg=f"Successfully parsed JSON. Response (logged to console):\n{pretty_json_response_str}"
        # )

        try:
            with open(file=json_file_path, mode="a", encoding="utf-8") as f_json:
                json.dump(obj=response_data, fp=f_json, ensure_ascii=False, indent=2)
            logger.info(msg=f"Successfully saved JSON response to: {json_file_path}")
        except IOError as e:
            logger.error(
                msg=f"Error writing JSON response to file {json_file_path}: {e}"
            )
            # Decide if this should prevent returning response_data.
            # For now, we still return it if parsing was successful.

        return response_data

    except requests.exceptions.JSONDecodeError:
        logger.error(msg="Response was not valid JSON.")
        logger.info(msg=f"Raw response text: {response_object.text}")
        return None  # JSON parsing failed
    except Exception as e:  # Catch any other unexpected errors during processing/saving
        logger.error(
            msg=f"An unexpected error occurred during process_and_save_json_response: {e}",
            exc_info=True,
        )
        return None
