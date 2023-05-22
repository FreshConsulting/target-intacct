from datetime import datetime
import json
import pandas as pd

import singer

from .utils import get_input, set_journal_entry_value

logger = singer.get_logger()


def statistical_journal_upload(intacct_client, object_name) -> None:
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

    # Journal Entries to be uploaded
    journal_entries = load_statistical_journal_entries(
        employee_ids, class_ids, location_ids, department_ids, object_name
    )

    # Post the journal entries to Intacct
    for entry in journal_entries:
        intacct_client.post_journal(entry)

    logger.info("Upload completed")


def load_statistical_journal_entries(
    employee_ids, class_ids, location_ids, department_ids, object_name
):
    """Loads inputted data into Statistical Journal Entries."""

    # Get input from pipeline
    input_value = get_input()

    # Convert input from dictionary to DataFrame
    data_frame = pd.DataFrame(input_value)

    # Verify it has required columns
    cols = list(data_frame.columns)
    REQUIRED_COLS = {
        "employeeid",
        "Capacity",
        "BudgetedBillable",
        "CapacityNo",
        "BudgetedBillableNo",
        "locationid",
        "PracticeAreaID",
        "BusinessUnit",
        "contact_name",
    }

    if not REQUIRED_COLS.issubset(cols):
        raise Exception(
            f"Input is missing REQUIRED_COLS. Found={json.dumps(cols)}, Required={json.dumps(REQUIRED_COLS)}"
        )

    # Build the entries
    journal_entries, errored = build_lines(
        data_frame,
        employee_ids,
        class_ids,
        location_ids,
        department_ids,
        object_name,
    )
    # If an error occurred when loading entries
    if errored:
        raise Exception("Building Statistical Journal Entries failed!")

    # Print journal entries
    logger.info(f"Loaded {len(journal_entries)} journal entries to post")

    return journal_entries


def build_entry(
    row,
    employee_ids,
    class_ids,
    location_ids,
    department_ids,
    object_name,
    account,
):
    
    accountNo = row[account + "No"]
    amount = row[account]

    employee_id = row["employeeid"]
    class_id = row["BusinessUnit"]
    location_id = row["locationid"]
    department_id = row["PracticeAreaID"]

    # Create journal entry line detail
    je_detail = {
        "AMOUNT": str(round(float(amount), 2)),
        "TR_TYPE": 1,
        "ACCOUNTNO": accountNo,
    }

    entry_error = False
    for lst, field, to_search in [
        (employee_ids, "EMPLOYEEID", employee_id),
        (class_ids, "CLASSID", class_id),
        (location_ids, "LOCATIONID", location_id),
        (department_ids, "DEPARTMENTID", department_id),
    ]:
        entry_error = set_journal_entry_value(
            je_detail, lst, field, to_search, object_name
        )
        if entry_error:
            break

    return je_detail, entry_error


def build_lines(
    data, employee_ids, class_ids, location_ids, department_ids, object_name
):
    logger.info(f"Converting {object_name}...")
    line_items = []
    journal_entries = []
    errored = False

    # Create line items
    line_item_types = ["Capacity", "BudgetedBillable"]

    for line_item_type in line_item_types:
        for index, row in data.iterrows():
            line_entry, errored = build_entry(row,
            employee_ids,
            class_ids,
            location_ids,
            department_ids,
            object_name,
            line_item_type)
            
            line_items.append(line_entry)
            
    # Create the entry
    entry = {
        "JOURNAL": row.get("Journal", "STJ"),
        "BATCH_DATE": datetime.now().strftime("%m/%d/%Y"),
        "BATCH_TITLE": "HOURS_PER_WEEK_DENOMINATOR",
        "ENTRIES": {"GLENTRY": line_items},
    }

    journal_entries.append(entry)

    return journal_entries, errored