REQUIRED_CONFIG_KEYS = [
    "company_id",
    "sender_id",
    "sender_password",
    "user_id",
    "user_password",
    "object_name",
    "entity_id",
]

# List of available objects with their internal object-reference/endpoint name.
INTACCT_OBJECTS = {
    "accounts_payable_bills": "APBILL",
    "general_ledger_accounts": "GLACCOUNT",
    "general_ledger_details": "GLDETAIL",
    "general_ledger_journal_entries": "GLBATCH",
    "general_ledger_journal_entry_lines": "GLENTRY",
    "employees": "EMPLOYEE",
    "classes": "CLASS",
    "locations": "LOCATION",
    "departments": "DEPARTMENT",
    "customers": "CUSTOMER",
    "projects": "PROJECT",
    "items": "ITEM",
    "vendors": "VENDOR",
    "statistical_accounts": "STATACCOUNT",
    "checking_accounts": "CHECKINGACCOUNT"
}

PAYMENT_RECORDS_REQUIRED_COLS = { "payouts": {"payout_id", "amount", "available_on"}, "transactions":  {"id", "payout_id", "amount", "fee", "tax", "transaction_type"}}

DEFAULT_API_URL = "https://api.intacct.com/ia/xml/xmlgw.phtml"
