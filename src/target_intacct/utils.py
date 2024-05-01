import io
import sys
import json
from typing import Dict, List

import singer

logger = singer.get_logger()


def get_input():
    """Read the input from the pipeline and return a dictionary of the Records."""
    input = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    input_value = []

    # For each line of input, if it has data content (is a record) add the line to the dictionary
    for row in input:
        try:
            raw_input = singer.parse_message(row).asdict()         
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(row))
            raise
        type = raw_input["type"]

        # Streams of type Record contain the inputted data
        if type == "RECORD" and not any(
            value == "" or value is None for value in raw_input["record"].values()
        ):
            # Group the data into dictionaries by stream name
            stream_name = raw_input["stream"]
            record = raw_input["record"] 
            
            # If a dictionary doesn't exist for the given stream name, create it
            if not any(dict.get("stream") == stream_name for dict in input_value):
                new_dict = {key: [value] for key, value in record.items()}
                new_dict["stream"] = stream_name
                input_value.append(new_dict)

            # Else add the values of the record to the existing dictionary
            else:
                for key, value in record.items(): 
                    existing_dict = [dict for dict in input_value if dict["stream"] == stream_name][0]
                    if key in existing_dict:                      
                        existing_dict[key].append(value)
                    else:
                        existing_dict[key] = [value]     
    return input_value


def set_journal_entry_value(
    je_detail: dict,
    intacct_values: List[Dict],
    field_name: str,
    search_value,
    object_name: str,
) -> bool:
    """Creates journal entries for statistical and financial journals."""
    if search_value and any(
        filter(lambda o: o.get(field_name) == str(search_value), intacct_values)
    ):
        je_field_name = (
            field_name
            if field_name
            in [
                "EMPLOYEEID",
                "CLASSID",
                "CUSTOMERID",
                "PROJECTID",
                "ITEMID",
                "VENDORID",
            ]
            else field_name.replace("ID", "")
        )
        je_detail[je_field_name] = search_value
    else:
        raise Exception(
            f"Field {field_name} with the value {search_value} is missing in Intacct for {object_name}"
        )
