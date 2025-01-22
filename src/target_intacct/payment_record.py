import pandas as pd
import datetime
import singer

from .utils import get_input

from .const import PAYMENT_RECORDS_REQUIRED_COLS, PAYMENT_RECORDS_REQUIRED_CONFIG_KEYS

logger = singer.get_logger()


def build_line_items(gross_amount, total_fees, total_sales_tax, config):
    """Builds the line items for Intacct uploads"""
    accountno = config["accountno_1"]
    details = {"memo": config["memo"], "locationid": config["locationid"], "departmentid": config["departmentid"], "projectid": config["projectid"], "customerid": config["customerid"], "classid": config["classid"]}
    lines = [{"glaccountno": accountno, "amount": gross_amount/100, **details}]
    if total_fees > 0:
        accountno = config["accountno_2"]
        lines.append({"glaccountno": accountno, "amount": -(total_fees/100), **details})
    if total_sales_tax > 0:
        accountno = config["accountno_3"]
        lines.append({"glaccountno": accountno, "amount": total_sales_tax/100, **details})

    return lines

def get_date_lines(year, month, day):
    """Get a formatted date line"""
    return { "year": year, "month": month, "day": day }

def get_receipt_data(transactions):
        """Get the total amount, fees, and taxes for a payout's transactions"""
        gross_amount, total_fees, total_adjustments, total_refunds, total_sales_tax = 0, 0, 0, 0, 0

        mapped_transactions = transactions.groupby("transaction_type")
        
        # Handles data by group (so all charges at once, all fees at once, etc)
        for group_name, group_df in mapped_transactions:        
            if group_name == "charge" or group_name == "payment":
                gross_amount += group_df["amount"].sum()
                total_fees += group_df["fee"].sum()               
                total_sales_tax += group_df["tax"].sum() 

            elif group_name == "fee":
                total_fees += group_df["amount"].sum() 
                total_fees += group_df["fee"].sum()

            elif group_name == "adjustment":
                total_adjustments += group_df["amount"].sum()
                total_fees += group_df["fee"].sum()

            elif group_name == "refund":
                total_refunds += group_df["amount"].sum()
                total_fees += group_df["fee"].sum()
            else:
                raise Exception(f"Unexpected transaction type found in transaction data. Found transaction type: {group_name}, expected: 'charge', 'payment', 'fee', 'adjustment', or 'refund' ")

        gross_amount = gross_amount + total_adjustments - total_refunds - total_sales_tax 
        return gross_amount, total_fees, total_sales_tax

def process_input(input_value): 
    """
    Process the recieved input
        Expected output: 
        Payouts: a dataframe containing all of the new payouts
            Format [{ "id": string, "amount": int, "available_on": timestamp }, ...]
        Transactions: a dataframe containing all of the new transactions
            Format [{"id": string, "amount": int, "fee": int, "transaction_type": string, "payout_id": string, "tax": int}, ...]
    """   
    input_data = {data["stream"]: data for data in input_value if data["stream"] in ["payouts", "transactions"]}
   
    # Verify required colums are met
    for data_type in input_data.values():            
        cols = data_type.keys()
        REQUIRED_COLS = PAYMENT_RECORDS_REQUIRED_COLS[data_type["stream"]]

        if not REQUIRED_COLS.issubset(cols):
            raise Exception(
                f"Input is missing REQUIRED_COLS. Found={cols}, Required={REQUIRED_COLS}"
            )
    
    payouts = input_data.get('payouts')
    transactions = input_data.get('transactions')
    transactions_df = pd.DataFrame(transactions)
    payouts_df = pd.DataFrame(payouts)

    return payouts_df, transactions_df   

def verify_config_values(intacct_client, config):
    """
    Checks if all required config values are found
    Checks if the values stored in the config ( location ids, vendor ids, ect ) are in the Intacct instance
    """

    config_keys = set(config.keys())
    if not PAYMENT_RECORDS_REQUIRED_CONFIG_KEYS.issubset(config_keys) or not any(key.startswith("accountno") for key in config_keys):
        raise Exception(f"Config File is Missing Required config value, Found={config_keys} Required={PAYMENT_RECORDS_REQUIRED_CONFIG_KEYS}")
    
    # Get ids from Intacct to verify values in the config file
    location_ids = intacct_client.get_entity(object_type="locations", fields=["LOCATIONID"])
    department_ids = intacct_client.get_entity(object_type="departments", fields=["DEPARTMENTID"])
    vendor_ids = intacct_client.get_entity(object_type="vendors", fields=["VENDORID"])
    account_ids = intacct_client.get_entity(object_type="general_ledger_accounts", fields=["ACCOUNTNO"])
    bank_account_ids = intacct_client.get_entity(object_type="checking_accounts", fields=["BANKACCOUNTID"])
    project_ids = intacct_client.get_entity(object_type="projects", fields=["PROJECTID"])
    customer_ids = intacct_client.get_entity(object_type="customers", fields=["CUSTOMERID"])
    class_ids = intacct_client.get_entity(object_type="classes", fields=["CLASSID"])

    for name, ids_list in {"locationid": location_ids, "departmentid": department_ids, "vendorid": vendor_ids, "bankaccountid": bank_account_ids, "projectid": project_ids, "customerid": customer_ids, "classid": class_ids}.items():
        config_value = config[name]       
        if not any(pair[name.upper()] == config_value for pair in ids_list):
                raise Exception(
                f"Field {name} with the value {config_value} is missing in Intacct"
        )        

    # Checks all of the account numbers in the config against the account numbers in Intacct
    account_numbers = [value for key, value in config.items() if key.startswith('accountno')]
    for account_no in account_numbers:
        if not any(int(pair["ACCOUNTNO"]) == account_no for pair in account_ids):
            raise Exception(
                f"Field glaccountid with the value {account_no} is missing in Intacct"
        )        

def payment_record_upload(intacct_client, config) -> None:
    """Creates payment records in Intacct.

    Retrieves objects from Intacct API for verifying input data
    Retrieves required data from input
    Sends entries for uploading to Intacct
    """

    logger.info("Starting upload.")
    
    # Verify config data
    verify_config_values(intacct_client, config)

    # Get input from pipeline
    input_value = get_input()
    
    if not input_value or not isinstance(input_value, list):
        raise Exception(f"Invalid input data recieved. Input data={input_value}")
    
    # Convert input from dictionary to DataFrames
    payouts, transactions = process_input(input_value) 
    grouped_transactions = transactions.groupby('payout_id')

    for payout_id, associated_transactions in grouped_transactions:         
    
        # Get all of the reciept totals 
        gross_amount, total_fees, total_sales_tax = get_receipt_data(associated_transactions) 

        # Get the payout data for the group of transactions
        payout = payouts.loc[payouts['payout_id'] == payout_id].squeeze()
        payout_amount = payout["amount"]

        timestamp = payout["available_on"]
        payout_date = datetime.datetime.fromtimestamp(timestamp)
        year = payout_date.year
        month = payout_date.month
        day = payout_date.day
        
        # If payout was negative (more was refunded/in fees than profit made) send to manual payments, otherwise send data to other receipts
        if(payout_amount > 0):
            # The key order in this dictionary in required for the Intacct API call to work correctly
            data = {
                    "paymentdate": get_date_lines(year, month, day),
                    "payee": config["source"],
                    "receiveddate": get_date_lines(year, month, day),
                    "paymentmethod": config["paymentmethod"],
                    "bankaccountid": config["bankaccountid"], # ENV var
                    "depositdate": get_date_lines(year, month, day),
                    "description": config["description"],
                    "receiptitems": {"lineitem": build_line_items(gross_amount, total_fees, total_sales_tax, config)}}
            intacct_client.post_other_receipt(data)
        else:
            # The key order in this dictionary in required for the Intacct API call to work correctly
            data = {
                    "bankaccountid": config["bankaccountid"],
                    "vendorid": config["vendorid"],
                    "memo": config["manual_payment_memo"],
                    "paymentmethod": config["paymentmethod"],
                    "checkdate": get_date_lines(year, month, day),
                    "checkno": config["checkno"],
                    "billno": f"{year}{month:02}{day:02}", # billno is is equal to the date of the payment
                    "payitems": {"payitem": {"glaccountno": config["accountno_1"], "paymentamount": abs(payout_amount)/100, "item1099": config["item1099"], "departmentid": config["departmentid"], "locationid": config["locationid"], "projectid": config["projectid"], "customerid": config["customerid"], "classid": config["classid"]}}}           
            intacct_client.post_manual_payment(data)