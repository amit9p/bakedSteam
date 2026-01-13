
{
  CCAccount.account_id: "IA_NULL_GUARD",
  CCAccount.posted_balance: None,
  CCAccount.reported_1099_amount: None,
  CustomerInformation.bankruptcy_court_case_status_code: None,
  CustomerInformation.bankruptcy_chapter_number: None,
}



# Build the rules first (they don't need BK/Reactivation to be non-null)
current_balance_expr = (
    when(
        (col(CCAccount.is_account_paid_in_full.str) == True) |
        (col(CCAccount.settled_in_full_notification.str) == True) |
        (col(CCAccount.pre_charge_off_settled_in_full_notification.str) == True) |
        (col(Recoveries.is_debt_sold.str) == True) |
        (col(CCAccount.posted_balance.str) < 0),
        lit(0),
    )
    .when(
        (lower(col(CustomerInformation.bankruptcy_court_case_status_code.str)) == constants.BankruptcyStatus.OPEN.value) &
        (col(CustomerInformation.bankruptcy_chapter_number.str).isin(
            constants.BankruptcyChapter.TWELVE.value,
            constants.BankruptcyChapter.THIRTEEN.value,
        )),
        lit(0),
    )
    .when(
        lower(col(CustomerInformation.bankruptcy_court_case_status_code.str)) == constants.BankruptcyStatus.DISCHARGED.value,
        lit(0),
    )
    .when(
        lower(col(CCAccount.reactivation_status.str)) == constants.ReactivationNotification.REACTIVATED.value.lower(),
        lit(0),
    )
    .otherwise(
        when(
            col(CCAccount.posted_balance.str).isNull() |
            col(CCAccount.reported_1099_amount.str).isNull(),
            lit(constants.DEFAULT_ERROR_INTEGER),
        ).otherwise(
            (col(CCAccount.posted_balance.str) - col(CCAccount.reported_1099_amount.str)).cast("int")
        )
    )
)

calculated_df = calculated_df.withColumn(BaseSegment.current_balance_amount.str, current_balance_expr)



# 10) Rule 1: PIF Notification = true -> should ZERO
{
    CCAccount.account_id: "A_PIF",
    CCAccount.is_account_paid_in_full: True,     # Rule 1
    CCAccount.settled_in_full_notification: False,
    CCAccount.pre_charge_off_settled_in_full_notification: False,
    CCAccount.posted_balance: 999,
    CCAccount.reported_1099_amount: 100,
    CCAccount.reactivation_status: None,
    CCAccount.is_debt_sold: False,
},

# 11) Rule 2: Pre-CO SIF Notification = true -> should ZERO
{
    CCAccount.account_id: "A_PRECO_SIF",
    CCAccount.is_account_paid_in_full: False,
    CCAccount.settled_in_full_notification: True,  # Rule 2
    CCAccount.pre_charge_off_settled_in_full_notification: False,
    CCAccount.posted_balance: 888,
    CCAccount.reported_1099_amount: 10,
    CCAccount.reactivation_status: None,
    CCAccount.is_debt_sold: False,
},

# 12) Rule 3: Post-CO SIF Notification = true -> should ZERO
{
    CCAccount.account_id: "A_POSTCO_SIF",
    CCAccount.is_account_paid_in_full: False,
    CCAccount.settled_in_full_notification: False,
    CCAccount.pre_charge_off_settled_in_full_notification: True,  # Rule 3
    CCAccount.posted_balance: 777,
    CCAccount.reported_1099_amount: 20,
    CCAccount.reactivation_status: None,
    CCAccount.is_debt_sold: False,
},

# 13) Rule 4: Asset Sales Notification = true -> should ZERO
# (In your code this is represented by CCAccount.is_debt_sold)
{
    CCAccount.account_id: "A_ASSET_SALE",
    CCAccount.is_account_paid_in_full: False,
    CCAccount.settled_in_full_notification: False,
    CCAccount.pre_charge_off_settled_in_full_notification: False,
    CCAccount.posted_balance: 666,
    CCAccount.reported_1099_amount: 30,
    CCAccount.reactivation_status: None,
    CCAccount.is_debt_sold: True,   # Rule 4
},

# 14) Rule 9: ELSE -> posted_balance - reported_1099_amount (should be NON-ZERO)
{
    CCAccount.account_id: "A_ELSE",
    CCAccount.is_account_paid_in_full: False,
    CCAccount.settled_in_full_notification: False,
    CCAccount.pre_charge_off_settled_in_full_notification: False,
    CCAccount.posted_balance: 1200,
    CCAccount.reported_1099_amount: 200,
    CCAccount.reactivation_status: None,
    CCAccount.is_debt_sold: False,
},

# 15) OPEN but chapter NOT 12/13 -> should go ELSE (rule 9), not rule 6
{
    CCAccount.account_id: "A_OPEN_11",
    CCAccount.is_account_paid_in_full: False,
    CCAccount.settled_in_full_notification: False,
    CCAccount.pre_charge_off_settled_in_full_notification: False,
    CCAccount.posted_balance: 900,
    CCAccount.reported_1099_amount: 100,
    CCAccount.reactivation_status: None,
    CCAccount.is_debt_sold: False,
},

# 16) NULL guard -> should DEFAULT_ERROR_INTEGER
# (Pick one “required” input and set it to None; earlier you used is_debt_sold: None)
{
    CCAccount.account_id: "A_NULL_GUARD",
    CCAccount.is_account_paid_in_full: False,
    CCAccount.settled_in_full_notification: False,
    CCAccount.pre_charge_off_settled_in_full_notification: False,
    CCAccount.posted_balance: 500,
    CCAccount.reported_1099_amount: 50,
    CCAccount.reactivation_status: None,
    CCAccount.is_debt_sold: None,  # triggers null-guard in your logic
},

{ Recoveries.account_id: "A_PIF" },
{ Recoveries.account_id: "A_PRECO_SIF" },
{ Recoveries.account_id: "A_POSTCO_SIF" },
{ Recoveries.account_id: "A_ASSET_SALE" },
{ Recoveries.account_id: "A_ELSE" },
{ Recoveries.account_id: "A_OPEN_11" },
{ Recoveries.account_id: "A_NULL_GUARD" },




# Not bankruptcy-driven cases
{
    CustomerInformation.account_id: "A_PIF",
    CustomerInformation.bankruptcy_court_case_status_code: None,
    CustomerInformation.bankruptcy_chapter_number: None,
},
{
    CustomerInformation.account_id: "A_PRECO_SIF",
    CustomerInformation.bankruptcy_court_case_status_code: None,
    CustomerInformation.bankruptcy_chapter_number: None,
},
{
    CustomerInformation.account_id: "A_POSTCO_SIF",
    CustomerInformation.bankruptcy_court_case_status_code: None,
    CustomerInformation.bankruptcy_chapter_number: None,
},
{
    CustomerInformation.account_id: "A_ASSET_SALE",
    CustomerInformation.bankruptcy_court_case_status_code: None,
    CustomerInformation.bankruptcy_chapter_number: None,
},

# ELSE case: choose any allowed status that doesn't zero out (CLOSED is fine)
{
    CustomerInformation.account_id: "A_ELSE",
    CustomerInformation.bankruptcy_court_case_status_code: "CLOSED",
    CustomerInformation.bankruptcy_chapter_number: "07",
},

# OPEN but chapter 11 -> should NOT hit rule 6
{
    CustomerInformation.account_id: "A_OPEN_11",
    CustomerInformation.bankruptcy_court_case_status_code: "OPEN",
    CustomerInformation.bankruptcy_chapter_number: "11",
},

# NULL guard (bankruptcy irrelevant)
{
    CustomerInformation.account_id: "A_NULL_GUARD",
    CustomerInformation.bankruptcy_court_case_status_code: None,
    CustomerInformation.bankruptcy_chapter_number: None,
},








