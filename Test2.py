
Account DOFD > 10 years
H: Not fully implemented in current generator logic
I: DOFD-based exclusion is still pending; DOFD_7LESS is present as TODO / placeholder and not finalized in active rule logic
Account is Live Test Account
H: Implemented in generator exclusion rules
I: Excluded when is_live_test_account == True
Account is Canadian
H: Implemented in generator exclusion rules
I: Excluded when upper(financial_portfolio_id) == "CA"
Non-active subscriber code
H: Not implemented in current generator rules; needs ECBR / upstream clarification
I: No explicit rule found in current generator logic for subscriber code fields
3rd Thursday of month
H: Implemented in generator trigger rules
I: Triggered when dayofweek(account_as_of_date) == 5 and dayofmonth(account_as_of_date) is between 15 and 21
CCC is changed
H: Implemented in generator trigger rules
I: Triggered when calculated_ccc != last_reported_ccc
Account is Sold
H: Implemented in generator trigger rules
I: Triggered when is_debt_sold == True
Manual trigger applied by CBR Business
H: Implemented in generator trigger rules
I: Triggered when is_credit_bureau_manual_reporting == True
Account moves to a Final Status attribute
H: Implemented in generator trigger rules
I: Triggered when account_status == "DA"
Account deleted
H: Not clearly implemented in current generator logic
I: No explicit delete trigger rule found in current rules.py
Account is Reactivated
H: Implemented in generator exclusion rules
I: Excluded when upper(reactivation_status) == "REACTIVATED"
Account under non-605B Fraud Investigation
H: Implemented in generator exclusion rules
I: Excluded when is_identity_fraud_claimed_on_account == True and block_notification != True
Account is manually suppressed
H: Implemented in generator exclusion rules
I: Excluded when is_credit_bureau_manual_suppressed == True
Account is in a final status
H: Implemented in generator trigger logic
I: Final-status handling is covered through account_status == "DA" trigger logic
Account is <30 days old
H: Implemented in generator exclusion rules
I: Excluded when datediff(current_date(), account_open_date) < 30
Account has already reported under non-605B Fraud Investigation
H: Implemented through current non-605B fraud exclusion logic
I: Exclusion is driven by is_identity_fraud_claimed_on_account == True and block_notification != True
Account under 605B Fraud investigation
H: Implemented in generator exclusion rules
I: Excluded when is_identity_fraud_claimed_on_account == True and block_notification == True
Account has already reported deleted
H: Not clearly implemented in current generator logic
I: No explicit already-reported-deleted exclusion rule found in current rules.py
Account has already reported Reactivated
H: Implemented through current reactivation exclusion logic
I: Excluded when upper(reactivation_status) == "REACTIVATED"
Account fails eDQ Check
H: EDQ suppression completed in generator
I: Final EDQ suppression is implemented in reportable_accounts.py by excluding accounts present in edq_suppressions_df using account_id
Also, for Katie, send this after sheet update:
Message
Updated the Generator I/P Readiness sheet based on the current generator implementation. I marked the items that are already implemented in generator rules and noted the ones still pending or not explicitly present in the current logic, like DOFD, non-active subscriber code, and delete-related handling.
