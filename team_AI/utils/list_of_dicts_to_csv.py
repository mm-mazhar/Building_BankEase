# -*- coding: utf-8 -*-
# """
# utils/list_of_dicts_to_csv.py
# Created on June 03, 2025
# @ Author: Mazhar
# """

import logging
import os
import pandas as pd
from typing import List, Dict, Any  # Ensure List, Dict are imported


def save_list_of_dicts_to_csv(
    data_list: List[Dict[str, Any]],
    csv_filepath: str,
    logger: logging.Logger,
) -> bool:
    """
    Converts a list of flat dictionaries to a Pandas DataFrame and saves it as a CSV.
    Overwrites the CSV file if it exists.

    Args:
        data_list (List[Dict[str, Any]]): The list of dictionaries to convert.
                                         Each dictionary represents a row.
        csv_filepath (str): Path to the output CSV file.
        logger (logging.Logger): Logger instance.

    Returns:
        bool: True if saving was successful, False otherwise.
    """
    if not csv_filepath:  # Check if path is provided
        logger.error(msg="CSV filepath not provided for saving.")
        return False

    logger.info(msg=f"Attempting to save data to CSV: {csv_filepath}")

    if not data_list:
        logger.info(msg="The provided data list is empty. Creating an empty CSV file.")
        try:
            df_empty = pd.DataFrame(data=data_list)  # Will create an empty DataFrame
            df_empty.to_csv(
                path_or_buf=csv_filepath,
                index=False,
                mode="w",  # Overwrite
                encoding="utf-8",
            )
            logger.info(msg=f"Empty CSV file '{csv_filepath}' created/overwritten.")
            return True
        except Exception as e:
            logger.error(
                msg=f"Error creating an empty CSV {csv_filepath}: {e}", exc_info=True
            )
            return False

    try:
        df = pd.DataFrame(data=data_list)
        logger.info(
            msg=f"Successfully created DataFrame for CSV with {len(df)} rows and {len(df.columns)} columns."
        )
        df.to_csv(
            path_or_buf=csv_filepath, index=False, mode="w", encoding="utf-8"
        )  # Overwrite
        logger.info(msg=f"Successfully saved DataFrame to CSV: {csv_filepath}")
        return True
    except Exception as e:
        logger.error(
            msg=f"An error occurred during DataFrame creation or CSV writing: {e}",
            exc_info=True,
        )
        return False
