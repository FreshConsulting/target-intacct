from dateutil.parser import parse
import pandas as pd

import singer

from .utils import get_input

logger = singer.get_logger()


def employee_rate_upload(intacct_client) -> None:
    """Creates employee rates in Intacct.

    Retrieves objects from Intacct API for verifying input data
    Calls load_entries method
    Sends entries for uploading to Intacct
    """

    logger.info("Starting upload.")

    # Load Current Data in Intacct for input verification
    employee_ids = intacct_client.get_entity(
        object_type="employees", fields=["EMPLOYEEID"]
    )
    ids = [a_dict["EMPLOYEEID"] for a_dict in employee_ids]
    
    # Get input from pipeline
    input_value = get_input()

    if not input_value or not isinstance(input_value, list):
        raise Exception(f"Invalid input data recieved. Input data={input_value}")
        
    # Convert input from dictionary to DataFrame
    data_frame = pd.DataFrame(input_value[0])

    # Verify it has required columns
    cols = list(data_frame.columns)
    REQUIRED_COLS = {"employeeid", "ratestartdate"}

    if not REQUIRED_COLS.issubset(cols):
        raise Exception(
            f"Input is missing REQUIRED_COLS. Found={cols}, Required={REQUIRED_COLS}"
        )

    for index, row in data_frame.iterrows():
        if str(row["employeeid"]) in ids:
            start_date = parse(row["ratestartdate"])
            year, month, day = start_date.year, start_date.month, start_date.day
            employee_rate = {
                    "employeeid": row["employeeid"],
                    "ratestartdate": {
                        "year": year,
                        "month": month,
                        "day": day
                    },
                    "billingrate": row.get("billingrate", ""),
                    "salaryrate": row.get("salaryrate", ""), 
            }
            intacct_client.post_employee_rate(employee_rate)
           
        else:
            raise Exception(
             f"Invalid Employee ID. Employee ID with the value {row['employeeid']} is missing in Intacct"
        )
    
    logger.info("Upload completed")
