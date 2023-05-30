# Target-Intacct
A custom Meltano target that sends data into Intacct.
- The target is based on HotGlueXYZ's [Target-Intacct](https://github.com/hotgluexyz/target-intacct)
- Currently supports Statistical Journals with base code for Standard Journals

## Install
- Clone the Repository 

- In the root folder run `pip install .`

## Testing
 - The target is utilized as a component of [Meltano](https://docs.meltano.com/getting-started/meltano-at-a-glance) pipelines so local changes can be tested by running a pipeline with the target installed as a loader. 

### Required config variables
Variables to be put in the config section of the loader in the Meltano pipeline's *meltano.yml* file
``` env
INTACCT_SENDER_ID=[SENDER_ID]
INTACCT_SENDER_PASSWORD=[SENDER_PASSWORD]
INTACCT_USER_ID=[WEB_USER_ID]
INTACCT_USER_PASSWORD=[WEB_USER_PASSWORD]
INTACCT_COMPANY_ID=[COMPANY_ID]
INTACCT_PROD_COMPANY_ID=[PROD_ID]
object_name
```
*object_name* dictates what Intacct object will be loaded into Intacct ex: statistical_journal
### Optional config variables
``` env
INTACCT_ENTITY_ID=[ENTITY_ID]
batch_title
```
*batch_title* dictates the Batch Title for the Journal or Statistical Journal to be uploaded. The Batch Title will be the same as the *object_name* if this field is ommitted
## Statistical Journals Input Data
The needed types and format of data to be inputed into the target to create a statistical journal. 
### Required Fields
- `tr_type` The transaction type. 1 for Increase, otherwise -1 for Decrease.
- At least one `accountno` corresponding `amount`.
    - `accountno` is the statistical account that the data refers to. For example, account number 00001 could be for the Blocks of Cheese account.
    - `amount` is the value to be reported for that account. For example, if amount = 8 that would mean there are 8 blocks of cheese.
    - The `accountno` and `amount` fields can have any desired name, as long as they both:
        1. Start with the base "accountno" and "amount" respectively. For example `accountnoCheese` or `amount23`
        2. Have the same name following the base. For example if the account number is named `accountnoCheese` the amount for that account has to be named `amountCheese`
    
### Optional Fields
- Additional `accountno` and `amount` pairs.
- Any Required Dimensions Standard to Intacct
    - `locationid` 
    - `departmentid`
    - `projectid` 
    - `customerid` 
    - `vendorid` 
    - `employeeid`
    - `itemid`
    - `classid`
- Additional information can be found in the [Intacct API Documentation](https://developer.intacct.com/api/general-ledger/stat-journal-entries/#create-statistical-journal-entry)