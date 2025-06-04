import json
import logging
from typing import Any, Callable, Optional

import requests


def get_plaid_access_token(
    institution_id: str,
    client_id: str,
    secret: str,
    initial_products: list[str],
    http_headers: dict[str, str],
    override_username: Optional[str],  # Passed directly
    override_password: Optional[str],  # Passed directly
    config: dict[str, Any],  # For URLs and possibly webhook
    logger: logging.Logger,
    fetch_response_func: Callable[
        [str, dict[str, str], dict[str, Any], logging.Logger],
        Optional[requests.Response],
    ],  # Pass your fetch_response utility
) -> Optional[str]:
    """
    Orchestrates the creation of a Plaid public token and its exchange for an access token.

    Args:
        institution_id (str): The ID of the institution.
        client_id (str): Plaid client ID.
        secret (str): Plaid secret.
        initial_products (list[str]): List of initial products.
        http_headers (dict[str, str]): HTTP headers for requests.
        override_username (Optional[str]): Sandbox override username.
        override_password (Optional[str]): Sandbox override password.
        config (dict[str, Any]): Configuration dictionary containing Plaid API URLs
                                 (BASE_URL, CREATE_PUBLIC_TOKEN_URL, EXCHANGE_TOKEN_URL)
                                 and WEBHOOK_URL.
        logger (logging.Logger): Logger instance.
        fetch_response_func (Callable[[
            str, dict[str, str], dict[str, Any], logging.Logger
        ], Optional[requests.Response]]): The utility function used to make API calls.
                                       Expected signature: (api_url, headers, payload, logger) -> Optional[requests.Response]


    Returns:
        Optional[str]: The access token if successful, otherwise None.
    """
    logger.info(
        msg=f"Running access token acquisition process for institution: {institution_id}"
    )

    # Step 1: Create Public Token
    pt_payload: dict[str, Any] = {
        "client_id": client_id,
        "secret": secret,
        "institution_id": institution_id,
        "initial_products": initial_products,
        "options": {
            "webhook": config.get(
                "WEBHOOK_URL", "https://www.example.com/webhook"
            ),  # Get from config or default
            "override_username": override_username,
            "override_password": override_password,
        },
    }

    create_public_token_url: Optional[str] = config.get("BASE_URL", "") + config.get(
        "CREATE_PUBLIC_TOKEN_URL", ""
    )
    if not create_public_token_url or not config.get("CREATE_PUBLIC_TOKEN_URL"):
        logger.error(
            msg=f"CREATE_PUBLIC_TOKEN_URL or BASE_URL missing in config for {institution_id}."
        )
        return None

    pt_response_obj: Optional[requests.Response] = fetch_response_func(
        create_public_token_url, http_headers, pt_payload, logger
    )

    public_token: Optional[str] = None
    if pt_response_obj and pt_response_obj.status_code == 200:
        try:
            pt_data: dict[str, Any] = pt_response_obj.json()
            public_token = pt_data.get("public_token")
            if public_token:
                # logger.info(msg=f"Public token obtained for {institution_id}.")
                pass
            else:
                logger.error(
                    msg=f"'public_token' key not found in response for {institution_id}. Response: {pt_data}"
                )
        except json.JSONDecodeError:
            logger.error(
                msg=f"Failed to decode JSON from public token response for {institution_id}. Status: {pt_response_obj.status_code}, Text: {pt_response_obj.text[:200]}..."
            )
    elif pt_response_obj:  # Response object exists but status code is not 200
        logger.error(
            msg=f"Failed to create public token for {institution_id}. Status: {pt_response_obj.status_code}, Text: {pt_response_obj.text[:200]}..."
        )
    else:  # fetch_response_func returned None
        logger.error(
            msg=f"No response received from public token creation API for {institution_id}."
        )

    if not public_token:
        logger.error(msg=f"Public token acquisition failed for {institution_id}.")
        return None

    # Step 2: Exchange for Access Token
    access_token: Optional[str] = None  # Initialize access_token for this scope
    exchange_payload: dict[str, Any] = {
        "client_id": client_id,
        "secret": secret,
        "public_token": public_token,
    }

    exchange_token_url: Optional[str] = config.get("BASE_URL", "") + config.get(
        "EXCHANGE_TOKEN_URL", ""
    )
    if not exchange_token_url or not config.get("EXCHANGE_TOKEN_URL"):
        logger.error(
            msg=f"EXCHANGE_TOKEN_URL or BASE_URL missing in config for {institution_id}."
        )
        return None

    # logger.info(
    #     msg=f"Attempting to exchange public token for access token for {institution_id} at {exchange_token_url}..."
    # )
    exchange_response_obj: Optional[requests.Response] = fetch_response_func(
        exchange_token_url, http_headers, exchange_payload, logger
    )

    if exchange_response_obj and exchange_response_obj.status_code == 200:
        try:
            exchange_data: dict[str, Any] = exchange_response_obj.json()
            access_token = exchange_data.get("access_token")
            if access_token:
                # logger.info(
                # #     msg=f"Access token obtained for {institution_id}: {access_token[:15]}..."
                # )
                pass
            else:
                logger.error(
                    msg=f"'access_token' key not found in exchange response for {institution_id}. Response: {exchange_data}"
                )
        except json.JSONDecodeError:
            logger.error(
                msg=f"Failed to decode JSON from access token exchange response for {institution_id}. Status: {exchange_response_obj.status_code}, Text: {exchange_response_obj.text[:200]}..."
            )
    elif exchange_response_obj:  # Response object exists but status code is not 200
        logger.error(
            msg=f"Failed to exchange for access token for {institution_id}. Status: {exchange_response_obj.status_code}, Text: {exchange_response_obj.text[:200]}..."
        )
    else:  # fetch_response_func returned None
        logger.error(
            msg=f"No response received from token exchange API for {institution_id}."
        )

    if not access_token:
        logger.error(msg=f"Access token acquisition failed for {institution_id}.")
        return None

    return access_token
