
consolidated_data = [{
    "account_id": "ACC001",
    "sor_id": "SOR001",
    "sor_customer_id": "CUST001",
    "account_as_of_date_of_data": None,
    "last_reported_compliance_condition_code": None,
    "is_account_paid_in_full": False,
    "is_account_terminal": False,
    "is_account_aged_debt": False,
    "settled_in_full_notification": False,
    "is_account_holder_deceased": False,
    "status_desc": "ACTIVE",
    "is_debt_sold": False,
    "is_credit_bureau_manual_reporting": False,
    "pre_charge_off_settled_in_full_notification": False,
    "account_open_date": None,
    "financial_portfolio_id": None,
    "reactivation_status": None,
    "is_identity_fraud_claimed_on_account": False,
    "block_notification": False,
}]

unified_data = [{
    "account_id": "ACC001",
    "sor_id": "SOR001",
    "sor_customer_id": "CUST001",
    "account_status": "11",
    "compliance_condition_code": None,
}]

consolidated_records = []
unified_records = []

for idx in range(1, count + 1):
    account_id = f"ACC{idx:03d}"
    sor_id = f"SOR{idx:03d}"
    customer_id = f"CUST{idx:03d}"

    consolidated_records.append({
        "account_id": account_id,
        "sor_id": sor_id,
        "sor_customer_id": customer_id,
        "account_as_of_date_of_data": None,
        "last_reported_compliance_condition_code": None,
        "is_account_paid_in_full": False,
        "is_account_terminal": False,
        "is_account_aged_debt": False,
        "settled_in_full_notification": False,
        "is_account_holder_deceased": False,
        "status_desc": "ACTIVE",
        "is_debt_sold": False,
        "is_credit_bureau_manual_reporting": False,
        "pre_charge_off_settled_in_full_notification": False,
        "account_open_date": None,
        "financial_portfolio_id": None,
        "reactivation_status": None,
        "is_identity_fraud_claimed_on_account": False,
        "block_notification": False,
    })

    unified_records.append({
        "account_id": account_id,
        "sor_id": sor_id,
        "sor_customer_id": customer_id,
        "account_status": "11",
        "compliance_condition_code": None,
    })



context.consolidated_df = context.spark.createDataFrame(
    consolidated_records,
    schema=consolidated_schema.get_structtype()
)

context.calculated_df = context.spark.createDataFrame(
    unified_records,
    schema=calculated_schema.get_structtype()
)


