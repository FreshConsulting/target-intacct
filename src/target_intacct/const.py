REQUIRED_CONFIG_KEYS = [
    "company_id",
    "entity_id",
    "object_name",
    "sender_id",
    "sender_password",
    "user_id",
    "user_password", 
]

PAYMENT_RECORDS_REQUIRED_CONFIG_KEYS = {
   "bankaccountid",
   "checkno", 
   "classid",
   "customerid",
   "departmentid", 
   "description", 
   "item1099", 
   "locationid", 
   "manual_payment_memo", 
   "memo", 
   "paymentmethod", 
   "projectid",
   "source",
   "vendorid",
}

# List of available objects with their internal object-reference/endpoint name.
INTACCT_OBJECTS = {
    "accounts_payable_bills": "APBILL",
    "checking_accounts": "CHECKINGACCOUNT",
    "classes": "CLASS",
    "customers": "CUSTOMER",
    "departments": "DEPARTMENT",
    "employees": "EMPLOYEE",
    "general_ledger_accounts": "GLACCOUNT",
    "general_ledger_details": "GLDETAIL",
    "general_ledger_journal_entries": "GLBATCH",
    "general_ledger_journal_entry_lines": "GLENTRY",
    "items": "ITEM",
    "locations": "LOCATION",
    "projects": "PROJECT",
    "statistical_accounts": "STATACCOUNT",
    "vendors": "VENDOR",
}

PAYMENT_RECORDS_REQUIRED_COLS = { 
    "payouts": {
        "payout_id", 
        "amount", 
        "available_on",
    }, 
    "transactions":  {
        "id", 
        "payout_id", 
        "amount", 
        "fee", 
        "tax", 
        "transaction_type",
    }
}

DEFAULT_API_URL = "https://api.intacct.com/ia/xml/xmlgw.phtml"
