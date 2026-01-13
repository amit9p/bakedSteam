


# ---------------------------
# CC_ACCOUNT (covers reactivation values + base fields)
# ---------------------------
cc_account_data = [
    # 1) Negative balance -> should ZERO (rule 5)
    {
        CCAccount.account_id: "A_NEG",
        CCAccount.is_account_paid_in_full: False,
        CCAccount.settled_in_full_notification: False,
        CCAccount.pre_charge_off_settled_in_full_notification: False,
        CCAccount.posted_balance: -10,
        CCAccount.reported_1099_amount: 999,
        CCAccount.reactivation_status: None,
        CCAccount.is_debt_sold: False,
    },

    # 2) Reactivation Notification = Reactivated (Confluence case) -> should ZERO (rule 8)
    {
        CCAccount.account_id: "A_REACT_TITLE",
        CCAccount.is_account_paid_in_full: False,
        CCAccount.settled_in_full_notification: False,
        CCAccount.pre_charge_off_settled_in_full_notification: False,
        CCAccount.posted_balance: 600,
        CCAccount.reported_1099_amount: 50,
        CCAccount.reactivation_status: "Reactivated",   # <-- matches Confluence UI value
        CCAccount.is_debt_sold: False,
    },

    # 3) Reactivation Notification = reactivated (normalized) -> should ZERO (rule 8)
    {
        CCAccount.account_id: "A_REACT_LOWER",
        CCAccount.is_account_paid_in_full: False,
        CCAccount.settled_in_full_notification: False,
        CCAccount.pre_charge_off_settled_in_full_notification: False,
        CCAccount.posted_balance: 650,
        CCAccount.reported_1099_amount: 20,
        CCAccount.reactivation_status: constants.ReactivationNotification.REACTIVATED.value,  # "reactivated"
        CCAccount.is_debt_sold: False,
    },

    # 4) Reactivation Notification = Reactivation Declined -> should go ELSE (not zeroed by rule 8)
    {
        CCAccount.account_id: "A_REACT_DECLINED",
        CCAccount.is_account_paid_in_full: False,
        CCAccount.settled_in_full_notification: False,
        CCAccount.pre_charge_off_settled_in_full_notification: False,
        CCAccount.posted_balance: 1000,
        CCAccount.reported_1099_amount: 100,
        CCAccount.reactivation_status: "Reactivation Declined",
        CCAccount.is_debt_sold: False,
    },

    # 5) Bankruptcy OPEN + Chapter 12 -> should ZERO (rule 6)
    {
        CCAccount.account_id: "A_OPEN_12",
        CCAccount.is_account_paid_in_full: False,
        CCAccount.settled_in_full_notification: False,
        CCAccount.pre_charge_off_settled_in_full_notification: False,
        CCAccount.posted_balance: 500,
        CCAccount.reported_1099_amount: 10,
        CCAccount.reactivation_status: None,
        CCAccount.is_debt_sold: False,
    },

    # 6) Bankruptcy OPEN + Chapter 13 -> should ZERO (rule 6)
    {
        CCAccount.account_id: "A_OPEN_13",
        CCAccount.is_account_paid_in_full: False,
        CCAccount.settled_in_full_notification: False,
        CCAccount.pre_charge_off_settled_in_full_notification: False,
        CCAccount.posted_balance: 700,
        CCAccount.reported_1099_amount: 15,
        CCAccount.reactivation_status: None,
        CCAccount.is_debt_sold: False,
    },

    # 7) Bankruptcy DISCHARGED -> should ZERO (rule 7)
    {
        CCAccount.account_id: "A_DISCHARGED",
        CCAccount.is_account_paid_in_full: False,
        CCAccount.settled_in_full_notification: False,
        CCAccount.pre_charge_off_settled_in_full_notification: False,
        CCAccount.posted_balance: 800,
        CCAccount.reported_1099_amount: 5,
        CCAccount.reactivation_status: None,
        CCAccount.is_debt_sold: False,
    },

    # 8) Bankruptcy statuses that are valid but NOT zeroing rules -> should go ELSE
    {
        CCAccount.account_id: "A_CLOSED",
        CCAccount.is_account_paid_in_full: False,
        CCAccount.settled_in_full_notification: False,
        CCAccount.pre_charge_off_settled_in_full_notification: False,
        CCAccount.posted_balance: 1200,
        CCAccount.reported_1099_amount: 200,
        CCAccount.reactivation_status: None,
        CCAccount.is_debt_sold: False,
    },
    {
        CCAccount.account_id: "A_DISMISSED",
        CCAccount.is_account_paid_in_full: False,
        CCAccount.settled_in_full_notification: False,
        CCAccount.pre_charge_off_settled_in_full_notification: False,
        CCAccount.posted_balance: 900,
        CCAccount.reported_1099_amount: 100,
        CCAccount.reactivation_status: None,
        CCAccount.is_debt_sold: False,
    },
    {
        CCAccount.account_id: "A_CONVERTED",
        CCAccount.is_account_paid_in_full: False,
        CCAccount.settled_in_full_notification: False,
        CCAccount.pre_charge_off_settled_in_full_notification: False,
        CCAccount.posted_balance: 1000,
        CCAccount.reported_1099_amount: 0,
        CCAccount.reactivation_status: None,
        CCAccount.is_debt_sold: False,
    },

    # 9) Bankruptcy status NULL -> should go ELSE (unless earlier rules trigger)
    {
        CCAccount.account_id: "A_BK_NULL",
        CCAccount.is_account_paid_in_full: False,
        CCAccount.settled_in_full_notification: False,
        CCAccount.pre_charge_off_settled_in_full_notification: False,
        CCAccount.posted_balance: 1100,
        CCAccount.reported_1099_amount: 100,
        CCAccount.reactivation_status: None,
        CCAccount.is_debt_sold: False,
    },
]


# ---------------------------
# RECOVERIES (just to satisfy join)
# ---------------------------
recoveries_data = [{Recoveries.account_id: row[CCAccount.account_id]} for row in cc_account_data]


# ---------------------------
# CUSTOMER_INFORMATION (covers bankruptcy statuses + chapters list)
# ---------------------------
customer_information_data = [
    # For A_NEG (negative balance) - bankruptcy irrelevant
    {
        CustomerInformation.account_id: "A_NEG",
        CustomerInformation.bankruptcy_court_case_status_code: None,
        CustomerInformation.bankruptcy_chapter_number: None,
    },

    # Reactivation cases - bankruptcy irrelevant
    {
        CustomerInformation.account_id: "A_REACT_TITLE",
        CustomerInformation.bankruptcy_court_case_status_code: None,
        CustomerInformation.bankruptcy_chapter_number: None,
    },
    {
        CustomerInformation.account_id: "A_REACT_LOWER",
        CustomerInformation.bankruptcy_court_case_status_code: None,
        CustomerInformation.bankruptcy_chapter_number: None,
    },
    {
        CustomerInformation.account_id: "A_REACT_DECLINED",
        CustomerInformation.bankruptcy_court_case_status_code: None,
        CustomerInformation.bankruptcy_chapter_number: None,
    },

    # OPEN + Chapter 12/13 (rule 6)
    {
        CustomerInformation.account_id: "A_OPEN_12",
        CustomerInformation.bankruptcy_court_case_status_code: "OPEN",
        CustomerInformation.bankruptcy_chapter_number: "12",
    },
    {
        CustomerInformation.account_id: "A_OPEN_13",
        CustomerInformation.bankruptcy_court_case_status_code: "OPEN",
        CustomerInformation.bankruptcy_chapter_number: "13",
    },

    # DISCHARGED (rule 7)
    {
        CustomerInformation.account_id: "A_DISCHARGED",
        CustomerInformation.bankruptcy_court_case_status_code: "DISCHARGED",
        CustomerInformation.bankruptcy_chapter_number: "11",   # chapter irrelevant for discharged rule
    },

    # Other allowed bankruptcy statuses (should go ELSE)
    {
        CustomerInformation.account_id: "A_CLOSED",
        CustomerInformation.bankruptcy_court_case_status_code: "CLOSED",
        CustomerInformation.bankruptcy_chapter_number: "07",
    },
    {
        CustomerInformation.account_id: "A_DISMISSED",
        CustomerInformation.bankruptcy_court_case_status_code: "DISMISSED",
        CustomerInformation.bankruptcy_chapter_number: "11",
    },
    {
        CustomerInformation.account_id: "A_CONVERTED",
        CustomerInformation.bankruptcy_court_case_status_code: "CONVERTED_BK_ACCT_NO_DATA_AVAIL",
        CustomerInformation.bankruptcy_chapter_number: None,
    },

    # BK status NULL (explicit)
    {
        CustomerInformation.account_id: "A_BK_NULL",
        CustomerInformation.bankruptcy_court_case_status_code: None,
        CustomerInformation.bankruptcy_chapter_number: None,
    },
]


# ---------------------------
# Build DataFrames
# ---------------------------
account_df = create_partially_filled_dataset(spark, CCAccount, data=cc_account_data)
recoveries_df = create_partially_filled_dataset(spark, Recoveries, data=recoveries_data)
customer_df = create_partially_filled_dataset(spark, CustomerInformation, data=customer_information_data)
