# -*- coding: utf-8 -*-
# """
# utils/get_cols_series.py
# Created on June 03, 2025
# @ Author: Mazhar
# """

import pandas as pd
import os


def get_col_series_from_csv(
    csv_filepath: str, column_name: str = "institution_id"
) -> list[str]:
    """
    Reads a CSV file and extracts unique, non-empty values from a specified
    column into a list of strings.

    Args:
        csv_filepath (str): The full path to the CSV file.
        column_name (str): The name of the column containing institution IDs.
                           Defaults to "institution_id".

    Returns:
        list[str]: A list of unique institution IDs. Returns an empty list if
                   the file or column is not found, or if an error occurs.
    """
    series_data: list[str] = []
    if not os.path.exists(path=csv_filepath):
        # In your main script, you'd use your logger
        print(f"Error: Institutions CSV file not found at {csv_filepath}")
        return series_data

    try:
        # Read the specific column as string to avoid type issues with numbers
        df: pd.DataFrame = pd.read_csv(
            filepath_or_buffer=csv_filepath,
            usecols=[column_name],
            dtype={column_name: str},
        )

        if column_name in df.columns:
            id_series: pd.Series[str] = df[column_name].dropna().astype(dtype=str)
            series_data = list(dict.fromkeys(id_series[id_series != ""].tolist()))
        else:
            print(f"Error: Column '{column_name}' not found in {csv_filepath}.")
    except pd.errors.EmptyDataError:
        print(f"Error: Institutions CSV file is empty at {csv_filepath}.")
    except ValueError as ve:
        print(
            f"Error: Column '{column_name}' not found in the CSV file {csv_filepath}. {ve}"
        )
    except Exception as e:
        print(
            f"An error occurred while reading the institutions CSV file {csv_filepath}: {e}"
        )
    return series_data
