


A2 – Account DOFD > 10 years
F: Yes
G: No
I: Not fully implemented in current generator logic
J: DOFD-based exclusion is still pending; DOFD_7LESS is present as TODO / placeholder and not finalized
A3 – Account is Live Test Account
F: Yes
G: Yes
I: Implemented in generator conditions
J: Excluded when is_live_test_account = True
A4 – Account is Canadian
F: Yes
G: Yes
I: Implemented in generator conditions
J: Excluded when upper(financial_portfolio_id) = "CA"
A5 – Non-active subscriber code
F: Yes
G: No
I: Not implemented in current generator rules; needs ECBR / upstream clarification
J: No explicit condition found in current generator logic for subscriber code fields
A6 – 3rd Thursday of month
F: Yes
G: No
I: Condition exists in generator logic, but intent / sheet mapping is still being confirmed
J: Current generator checks dayofweek and dayofmonth for third-Thursday logic; final sheet status pending intent confirmation
A7 – CCC is changed
F: Yes
G: No
I: Need dev work from ECBR before we can test. See CT4018T-525
J: Awaiting CCC-related implementation / finalized condition before generator tracking can be marked implemented
A8 – Account is Sold
F: Yes
G: Yes
I: Implemented in generator conditions
J: Triggered when is_debt_sold = True
A9 – Manual trigger applied by CBR Business
F: Yes
G: Yes
I: Implemented in generator conditions
J: Triggered when is_credit_bureau_manual_reporting = True
A10 – Account moves to a Final Status attribute
F: Yes
G: Yes
I: Implemented in generator conditions
J: Covered through current final-status handling using the account_status = "DA" condition
A11 – Account deleted
F: Yes
G: No
I: Not clearly implemented in current generator logic
J: No explicit delete-specific condition found in current generator rules.py
A12 – Account is Reactivated
F: No
G: Yes
I: Implemented in generator conditions; intent wording still being aligned
J: Current generator logic checks reactivation using reactivation_status and related reactivation fields
A13 – Account under non-605B Fraud Investigation
F: Yes
G: Yes
I: Implemented in generator conditions
J: Excluded when is_identity_fraud_claimed_on_account = True and block_notification != True
A14 – Account is manually suppressed
F: Yes
G: Yes
I: Implemented in generator conditions
J: Excluded when is_credit_bureau_manual_suppressed = True
A16 – Account is <30 days old
F: Yes
G: Yes
I: Implemented in generator conditions
J: Excluded when datediff(current_date(), account_open_date) < 30
A17 – Account has already reported under non-605B Fraud Investigation
F: Yes
G: Yes
I: Implemented through current non-605B fraud handling
J: Condition is driven by is_identity_fraud_claimed_on_account = True and block_notification != True
A18 – Account under 605B Fraud Investigation
F: Yes
G: Yes
I: Implemented in generator conditions
J: Excluded when is_identity_fraud_claimed_on_account = True and block_notification = True
A19 – Account has already reported deleted
F: Yes
G: Yes
I: Implemented in generator conditions
J: Covered through current final-status / account_status = "DA" handling
A20 – Account has already reported Reactivated
F: No
G: Yes
I: Implemented through current reactivation handling; intent wording still being aligned
J: Current generator logic checks reactivation using reactivation_status and related reactivation fields
A21 – Account fails eDQ Check
F: Yes
G: Yes
I: EDQ work is completed at CT4018T-491
J: Final EDQ suppression is implemented in reportable_accounts.py by excluding accounts present in edq_suppressions_df
