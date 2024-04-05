from datetime import datetime
import pandas as pd

import singer

from .utils import get_input, set_journal_entry_value

logger = singer.get_logger()


def statistical_journal_upload(intacct_client, object_name, batch_title) -> None:
    """Uploads Statistical Journals to Intacct.

    Retrieves objects from Intacct API for verifying input data
    Calls load_entries method
    Sends entries for uploading to Intacct
    """

    logger.info("Starting upload.")

    # Load Current Data in Intacct for input verification
    employee_ids = intacct_client.get_entity(
        object_type="employees", fields=["EMPLOYEEID"]
    )
    class_ids = intacct_client.get_entity(object_type="classes", fields=["CLASSID"])
    location_ids = intacct_client.get_entity(
        object_type="locations", fields=["LOCATIONID"]
    )
    department_ids = intacct_client.get_entity(
        object_type="departments", fields=["DEPARTMENTID"]
    )
    customer_ids = intacct_client.get_entity(
        object_type="customers", fields=["CUSTOMERID"]
    )
    project_ids = intacct_client.get_entity(
        object_type="projects", fields=["PROJECTID"]
    )
    item_ids = intacct_client.get_entity(object_type="items", fields=["ITEMID"])
    vendor_ids = intacct_client.get_entity(object_type="vendors", fields=["VENDORID"])
    statistical_account_numbers = intacct_client.get_entity(
        object_type="statistical_accounts", fields=["ACCOUNTNO"]
    )

    dimension_values = {
        "employeeid": employee_ids,
        "classid": class_ids,
        "locationid": location_ids,
        "departmentid": department_ids,
        "customerid": customer_ids,
        "projectid": project_ids,
        "itemid": item_ids,
        "vendorid": vendor_ids,
    }

    # Journal Entries to be uploaded
    journal_entries = load_statistical_journal_entries(
        dimension_values,
        statistical_account_numbers,
        object_name,
        batch_title,
    )

    # Post the journal entries to Intacct
    for entry in journal_entries:
        intacct_client.post_journal(entry)

    logger.info("Upload completed")


def load_statistical_journal_entries(
    dimension_values,
    statistical_account_numbers,
    object_name,
    batch_title,
):
    """Loads inputted data into Statistical Journal Entries."""

    # Get input from pipeline
    input_value = get_input()

    if not input_value or not isinstance(input_value, list):
        raise Exception(f"Invalid input data recieved. Input data={input_value}")
    
    # Convert input from dictionary to DataFrame
    data_frame = pd.DataFrame(input_value[0])

    # Verify it has required columns
    cols = list(data_frame.columns)
    REQUIRED_COLS = {"tr_type"}

    if not REQUIRED_COLS.issubset(cols):
        raise Exception(
            f"Input is missing REQUIRED_COLS. Found={cols}, Required={REQUIRED_COLS}"
        )

    # Build the entries
    journal_entries = build_lines(
        data_frame,
        dimension_values,
        statistical_account_numbers,
        object_name,
        batch_title,
    )

    # Print journal entries
    logger.info(f"Loaded {len(journal_entries)} journal entries to post")

    return journal_entries


def build_entry(
    row,
    dimension_values,
    statistical_account_numbers,
    object_name,
    account_number_column,
):
    account_no = row[account_number_column]
    tr_type = row["tr_type"]

    # Get corresponding amount for current account
    try:
        amount = row[account_number_column.replace("accountno", "amount")]
    except KeyError:
        raise Exception(
            f"Statistical Account Number {account_no} is missing a corresponding amount"
        )

    # Create journal entry line detail
    je_detail = {
        "AMOUNT": str(round(amount, 2)),
        "TR_TYPE": tr_type,
    }

    set_journal_entry_value(
        je_detail, statistical_account_numbers, "ACCOUNTNO", account_no, object_name
    )
    
    for field_name, ids_list in dimension_values.items():
        if field_name in row.index:
            field = row[field_name]
            set_journal_entry_value(
                je_detail, ids_list, field_name.upper(), field, object_name
            )

    return je_detail


def build_lines(
    data,
    dimension_values,
    statistical_account_numbers,
    object_name,
    batch_title,
):
    logger.info(f"Converting {object_name}...")
    line_items = []
    journal_entries = []

    # Create list of account data the journal will contain
    account_number_columns = [
        column for column in data.columns if column.startswith("accountno")
    ]

    if len(account_number_columns):
        for account_number_column in account_number_columns:
            for index, row in data.iterrows():
                line_entry = build_entry(
                    row,
                    dimension_values,
                    statistical_account_numbers,
                    object_name,
                    account_number_column,
                )

                line_items.append(line_entry)

        # Create the entry
        entry = {
            "JOURNAL": row.get("Journal", "STJ"),
            "BATCH_DATE": datetime.now().strftime("%m/%d/%Y"),
            "BATCH_TITLE": batch_title,
            "ENTRIES": {"GLENTRY": line_items},
        }

        journal_entries.append(entry)
    else:
        raise Exception(
            "Missing Required accountno Column. At least one Account number is required to upload a journal"
        )

    return journal_entries
