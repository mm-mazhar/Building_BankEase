# -*- coding: utf-8 -*-
# """
# utils/flattened_data.py
# Created on June 04, 2025
# @ Author: Mazhar
# """

import json
import logging
from typing import Any, Dict, List, Optional

import pandas as pd


def flatten_identity_data_to_list_of_dicts(
    all_raw_responses: List[
        Dict[str, Any]
    ],  # List of full responses from /identity/get
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """
    Flattens data from Plaid's /identity/get endpoint responses.
    Each row in the output list represents one owner's identity information,
    enriched with account and item details.

    Args:
        all_raw_responses (List[Dict[str, Any]]): A list where each element is the
            raw JSON dictionary response from an /identity/get API call.
        logger (logging.Logger): Logger instance.

    Returns:
        List[Dict[str, Any]]: A list of flat dictionaries, each suitable for a CSV row.
    """
    flat_identity_records: List[Dict[str, Any]] = []
    logger.info(
        f"Starting to flatten identity data from {len(all_raw_responses)} API responses."
    )

    for response_index, raw_response in enumerate(all_raw_responses):
        if not isinstance(raw_response, dict):
            logger.warning(
                f"Skipping response at index {response_index} as it's not a dictionary."
            )
            continue

        item_info = raw_response.get("item", {})
        accounts = raw_response.get("accounts", [])
        request_id = raw_response.get("request_id", f"N/A_resp_idx_{response_index}")

        if not accounts:
            logger.info(
                f"No accounts found in response with request_id: {request_id}. Skipping."
            )
            continue

        for account in accounts:
            if not isinstance(account, dict):
                logger.warning(
                    f"Skipping an account in response {request_id} as it's not a dictionary."
                )
                continue

            owners = account.get("owners", [])
            if not owners:
                # Create a record for the account even if no owners listed (might indicate data issue or specific account type)
                base_record: Dict[str, Any] = {
                    "request_id_source": request_id,
                    "item_id": item_info.get("item_id"),
                    "item_institution_id": item_info.get("institution_id"),
                    "item_institution_name": item_info.get("institution_name"),
                    "account_id": account.get("account_id"),
                    "account_name": account.get("name"),
                    "account_official_name": account.get("official_name"),
                    "account_type": account.get("type"),
                    "account_subtype": account.get("subtype"),
                    "account_mask": account.get("mask"),
                    "account_balance_available": account.get("balances", {}).get(
                        "available"
                    ),
                    "account_balance_current": account.get("balances", {}).get(
                        "current"
                    ),
                    "account_balance_currency": account.get("balances", {}).get(
                        "iso_currency_code"
                    ),
                    "owner_name": None,  # No owner info
                    "owner_address_street": None,
                    "owner_address_city": None,
                    "owner_address_postal_code": None,
                    "owner_address_country": None,
                    "owner_address_primary": None,
                    "owner_email_data": None,
                    "owner_email_primary": None,
                    "owner_email_type": None,
                    "owner_phone_data": None,
                    "owner_phone_primary": None,
                    "owner_phone_type": None,
                }
                flat_identity_records.append(base_record)
                continue

            for owner_index, owner in enumerate(owners):
                if not isinstance(owner, dict):
                    logger.warning(
                        f"Skipping an owner for account {account.get('account_id')} as it's not a dictionary."
                    )
                    continue

                # Base record for each owner
                base_record: Dict[str, Any] = {
                    "request_id_source": request_id,
                    "item_id": item_info.get("item_id"),
                    "item_institution_id": item_info.get(
                        "institution_id"
                    ),  # This is the ID from Plaid for the bank
                    "item_institution_name": item_info.get("institution_name"),
                    # "original_queried_institution_id": item_info.get("institution_id_source"), # If you added this earlier
                    "account_id": account.get("account_id"),
                    "account_name": account.get("name"),
                    "account_official_name": account.get("official_name"),
                    "account_type": account.get("type"),
                    "account_subtype": account.get("subtype"),
                    "account_mask": account.get("mask"),
                    "account_balance_available": account.get("balances", {}).get(
                        "available"
                    ),
                    "account_balance_current": account.get("balances", {}).get(
                        "current"
                    ),
                    "account_balance_currency": account.get("balances", {}).get(
                        "iso_currency_code"
                    ),
                    # Add other account-level fields you need
                }

                # Owner names (Plaid returns a list, often with one name)
                owner_names = owner.get("names", [])
                base_record["owner_name"] = (
                    owner_names[0] if owner_names else None
                )  # Take the first name

                # Addresses (Take the primary, or first if no primary)
                primary_address = None
                first_address_data = None
                for addr in owner.get("addresses", []):
                    if addr.get("primary"):
                        primary_address = addr.get("data", {})
                        break
                    if not first_address_data:  # store first one as fallback
                        first_address_data = addr.get("data", {})

                chosen_address = (
                    primary_address
                    if primary_address
                    else (first_address_data if first_address_data else {})
                )
                base_record["owner_address_street"] = chosen_address.get("street")
                base_record["owner_address_city"] = chosen_address.get("city")
                base_record["owner_address_region"] = chosen_address.get("region")
                base_record["owner_address_postal_code"] = chosen_address.get(
                    "postal_code"
                )
                base_record["owner_address_country"] = chosen_address.get("country")
                base_record["owner_address_primary"] = (
                    True if primary_address else (False if first_address_data else None)
                )

                # Emails (Take primary, or first)
                primary_email = None
                first_email_data = None
                for email_obj in owner.get("emails", []):
                    if email_obj.get("primary"):
                        primary_email = email_obj
                        break
                    if not first_email_data:
                        first_email_data = email_obj

                chosen_email = (
                    primary_email
                    if primary_email
                    else (first_email_data if first_email_data else {})
                )
                base_record["owner_email_data"] = chosen_email.get("data")
                base_record["owner_email_primary"] = chosen_email.get("primary")
                base_record["owner_email_type"] = chosen_email.get("type")

                # Phone numbers (Take primary, or first mobile, or first anything)
                primary_phone = None
                first_mobile_phone = None
                first_any_phone = None
                for phone_obj in owner.get("phone_numbers", []):
                    if phone_obj.get("primary"):
                        primary_phone = phone_obj
                        break
                    if not first_mobile_phone and phone_obj.get("type") == "mobile":
                        first_mobile_phone = phone_obj
                    if not first_any_phone:
                        first_any_phone = phone_obj

                chosen_phone = (
                    primary_phone
                    if primary_phone
                    else (
                        first_mobile_phone
                        if first_mobile_phone
                        else (first_any_phone if first_any_phone else {})
                    )
                )
                base_record["owner_phone_data"] = chosen_phone.get("data")
                base_record["owner_phone_primary"] = chosen_phone.get("primary")
                base_record["owner_phone_type"] = chosen_phone.get("type")

                flat_identity_records.append(base_record)

    logger.info(
        f"Finished flattening. Generated {len(flat_identity_records)} records for CSV."
    )
    return flat_identity_records


def flatten_plaid_transactions_data(
    full_data: dict[str, Any], logger_instance: logging.Logger
) -> list[dict[str, Any]]:
    """
    Flattens the consolidated Plaid data (transactions, accounts, item)
    into a list of dictionaries suitable for a single CSV.
    Each dictionary represents one transaction enriched with account and item info.
    """
    flat_list: list[dict[str, Any]] = []

    accounts_list: list[dict[str, Any]] = full_data.get("accounts", [])
    transactions_list: list[dict[str, Any]] = full_data.get("transactions", [])
    item_info: dict[str, Any] = full_data.get(
        "item", {}
    )  # Plaid /transactions/get includes an 'item' object
    # If your 'full_data' doesn't have 'item' at top level, but was added to 'metadata', adjust access.
    # For example, item_info = full_data.get("metadata", {}).get("item_details_for_institution_X", {})

    account_lookup: dict[str, dict[str, Any]] = {
        acc["account_id"]: acc for acc in accounts_list
    }

    logger_instance.info(msg=f"Flattening {len(transactions_list)} transactions.")

    for tx in transactions_list:
        row: dict[str, Any] = {}

        # 1. Add all transaction fields (optionally prefixed)
        for key, value in tx.items():
            if isinstance(
                value, dict
            ):  # e.g., personal_finance_category, location, payment_meta
                for sub_key, sub_value in value.items():
                    if isinstance(sub_value, (dict, list)):  # Further nesting
                        row[f"tx_{key}_{sub_key}"] = json.dumps(obj=sub_value)
                    else:
                        row[f"tx_{key}_{sub_key}"] = sub_value
            elif isinstance(value, list):  # e.g., counterparties
                # Option 1: Serialize the whole list
                # row[f"tx_{key}"] = json.dumps(value)
                # Option 2: Extract specific elements or aggregate
                if key == "counterparties" and value:
                    row[f"tx_{key}_0_name"] = value[0].get("name")
                    row[f"tx_{key}_0_type"] = value[0].get("type")
                    # Add more counterparty fields or loop if multiple needed
            else:
                row[f"tx_{key}"] = value

        # 2. Add relevant account fields
        account_id: Any | None = tx.get("account_id")
        if account_id and account_id in account_lookup:
            acc: dict[str, Any] = account_lookup[account_id]
            row["acc_account_id"] = acc.get("account_id")  # Explicitly add for clarity
            row["acc_name"] = acc.get("name")
            row["acc_official_name"] = acc.get("official_name")
            row["acc_subtype"] = acc.get("subtype")
            row["acc_type"] = acc.get("type")
            balances = acc.get("balances", {})
            row["acc_balance_available"] = balances.get("available")
            row["acc_balance_current"] = balances.get("current")
            row["acc_balance_iso_currency_code"] = balances.get("iso_currency_code")
            row["acc_balance_limit"] = balances.get("limit")
            row["acc_balance_unofficial_currency_code"] = balances.get(
                "unofficial_currency_code"
            )
            row["acc_holder_category"] = acc.get("holder_category")
            row["acc_mask"] = acc.get("mask")
            row["acc_institution_id_source"] = acc.get(
                "institution_id_source"
            )  # If you added this

        # 3. Add item-level fields (will be repeated for all transactions of this item)
        # This assumes item_info is relevant for all transactions being processed.
        # If 'item' data was collected per institution, ensure it's associated correctly.
        # For this example, let's assume `item_info` is passed if it was part of the single full response.
        # If `full_data` is truly a mix from many items, this part is more complex.
        # The original Plaid response for /transactions/get has ONE 'item' object.
        if item_info:
            row["item_id"] = item_info.get("item_id")
            row["item_institution_id"] = item_info.get("institution_id")
            row["item_webhook"] = item_info.get("webhook")
            # Add other item fields as needed.

        # Ensure the source institution ID for the transaction itself is present
        if "institution_id_source" not in row and "tx_institution_id_source" in row:
            row["institution_id_source"] = row["tx_institution_id_source"]
        elif "institution_id_source" not in row and item_info.get("institution_id"):
            row["institution_id_source"] = item_info.get("institution_id")

        flat_list.append(row)

    return flat_list
