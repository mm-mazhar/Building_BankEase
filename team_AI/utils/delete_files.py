# -*- coding: utf-8 -*-
# """
# utils/delete_files.py
# Created on June 03, 2025
# @ Author: Mazhar
# """

import logging
import os
from typing import Optional


def delete_files_if_exist(
    file_paths: list[Optional[str]],  # Accept a single list of optional paths
    logger: logging.Logger,
) -> None:
    """
    Iterates through a list of file paths and deletes each file if it exists.

    Args:
        file_paths (list[Optional[str]]): A list of file paths to delete.
                                          Paths can be None, which will be skipped.
        logger (logging.Logger): Logger instance for logging messages.
    """
    if not file_paths:
        logger.info(msg="No file paths provided for deletion.")
        return

    for file_path in file_paths:
        if file_path and os.path.exists(
            path=file_path
        ):  # Check if path is not None and file exists
            try:
                os.remove(path=file_path)
                logger.info(msg=f"Successfully removed existing file: {file_path}")
            except OSError as e:
                logger.error(
                    msg=f"Error removing existing file {file_path}: {e}",
                    exc_info=True,  # Provides traceback information
                )
            except Exception as e:  # Catch any other unexpected errors during removal
                logger.error(
                    msg=f"An unexpected error occurred while trying to remove {file_path}: {e}",
                    exc_info=True,
                )
        elif file_path:  # Path was provided but file doesn't exist
            logger.info(msg=f"File not found (no need to delete): {file_path}")
        # If file_path is None, it's skipped silently, or you could log it:
        # else:
        #     logger.debug("Skipped a None file_path in deletion list.")
