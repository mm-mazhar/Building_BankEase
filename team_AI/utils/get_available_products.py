# -*- coding: utf-8 -*-
# """
# utils//get_available_products.py
# Created on June 04, 2025
# @ Author: Mazhar
# """

import pandas as pd
import os
from typing import List, Optional


def get_available_products_for_institution(
    csv_filepath: str,
    target_institution_id: str,
    institution_id_col: str = "institution_id",
    product_col: str = "available_product",
) -> list[str]:
    """
    Reads an items CSV file and retrieves a list of unique, non-empty
    available products for a specific institution ID.

    Args:
        csv_filepath (str): The full path to the CSV file.
        target_institution_id (str): The institution_id to filter by.
        institution_id_col (str): The name of the column containing institution IDs.
        product_col (str): The name of the column containing available products.

    Returns:
        list[str]: A list of unique available products for the given institution ID.
                   Returns an empty list if the institution ID is not found,
                   the file doesn't exist, columns are missing, or an error occurs.
    """
    if not os.path.exists(path=csv_filepath):
        print(f"Error: CSV file not found at {csv_filepath}")  # Use logger in your app
        return []
    if not target_institution_id:
        print("Error: target_institution_id cannot be empty.")  # Use logger
        return []

    try:
        # Read only necessary columns and ensure they are read as strings
        df: pd.DataFrame = pd.read_csv(
            filepath_or_buffer=csv_filepath,
            usecols=[institution_id_col, product_col],
            dtype={institution_id_col: str, product_col: str},
        )

        # Filter for the target institution ID
        institution_df: pd.DataFrame = df[
            df[institution_id_col] == target_institution_id
        ]

        if institution_df.empty:
            # print(f"No data found for institution_id: {target_institution_id}") # Use logger
            return []

        # Get the 'available_product' series, drop NaNs/None, filter out empty strings, get unique
        products_series: pd.Series = institution_df[product_col].dropna()
        # Using dict.fromkeys to get unique while preserving order (if that matters)
        available_products = list(
            dict.fromkeys(products_series[products_series != ""].tolist())
        )

        # print(f"Found {len(available_products)} available products for institution {target_institution_id}: {available_products}") # Use logger
        return available_products

    except pd.errors.EmptyDataError:
        print(f"Error: CSV file is empty at {csv_filepath}.")
    except ValueError as ve:  # Handles case where columns in usecols are not in file
        print(
            f"Error: One or more required columns ('{institution_id_col}', '{product_col}') not found in {csv_filepath}. {ve}"
        )
    except Exception as e:
        print(
            f"An error occurred while processing {csv_filepath} for institution {target_institution_id}: {e}"
        )
    return []


# # --- Example Usage ---
# if __name__ == "__main__":
#     DEMO_ITEMS_CSV_PATH = "./temp_data/items_for_product_lookup.csv"
#     create_dummy_items_csv_if_not_exists(DEMO_ITEMS_CSV_PATH)

#     print("\n--- Test Cases ---")

#     # Test case 1: Institution ID exists with multiple products
#     inst_id_1 = "ins_122878"
#     products_1 = get_available_products_for_institution(DEMO_ITEMS_CSV_PATH, inst_id_1)
#     print(f"Products for {inst_id_1}: {products_1}")
#     # Expected: ['assets', 'auth', 'balance', 'identity', 'identity_match', 'income_verification', 'payment_initiation', 'recurring_transactions', 'signal', 'transfer']

#     # Test case 2: Institution ID exists with one product
#     inst_id_2 = "ins_002"
#     products_2 = get_available_products_for_institution(DEMO_ITEMS_CSV_PATH, inst_id_2)
#     print(f"Products for {inst_id_2}: {products_2}")
#     # Expected: ['balance']

#     # Test case 3: Institution ID exists but has no available products (NaN/None in CSV)
#     inst_id_3 = "ins_001"
#     products_3 = get_available_products_for_institution(DEMO_ITEMS_CSV_PATH, inst_id_3)
#     print(f"Products for {inst_id_3}: {products_3}")
#     # Expected: []

#     # Test case 4: Institution ID does not exist in the CSV
#     inst_id_4 = "ins_non_existent"
#     products_4 = get_available_products_for_institution(DEMO_ITEMS_CSV_PATH, inst_id_4)
#     print(f"Products for {inst_id_4}: {products_4}")
#     # Expected: []

#     # Test case 5: CSV file does not exist
#     non_existent_csv = "./temp_data/does_not_exist.csv"
#     products_5 = get_available_products_for_institution(non_existent_csv, "ins_122878")
#     print(f"Products from non-existent CSV: {products_5}")
#     # Expected: [] Error message, then []

#     # Test case 6: Empty target_institution_id
#     products_6 = get_available_products_for_institution(DEMO_ITEMS_CSV_PATH, "")
#     print(f"Products for empty institution ID: {products_6}")
#     # Expected: Error message, then []

#     # Clean up dummy directory (optional)
#     # import shutil
#     # if os.path.exists("./temp_data"):
#     #     shutil.rmtree("./temp_data")
#     #     print("Cleaned up dummy directory: ./temp_data")
