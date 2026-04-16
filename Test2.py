1. Account DOFD > 10 years
H Notes: Implemented in generator exclusion rules
I Logic: Excluded when aged debt / DOFD > 7 years rule condition is met per generator exclusion logic
2. Account is Live Test Account
H Notes: Implemented in generator exclusion rules
I Logic: Excluded when is_live_test_account = true
3. Account is Canadian
H Notes: Implemented in generator exclusion rules
I Logic: Excluded when financial_portfolio_id indicates Canada
4. Non-active subscriber code
H Notes: Implemented using subscriber code based exclusion logic
I Logic: Excluded based on bureau/subscriber code conditions
5. 3rd Thursday of month
H Notes: Implemented in trigger rules
I Logic: Triggered when account_as_of_date_of_data falls on the third Thursday of the month
6. CCC is changed
H Notes: Implemented in trigger rules; confirm business intent if needed
I Logic: Triggered when calculated CCC differs from last reported CCC
7. Account is Sold
H Notes: Implemented in trigger rules
I Logic: Triggered when is_debt_sold = true
8. Manual trigger applied by CBR Business
H Notes: Implemented in trigger rules
I Logic: Triggered when manual credit bureau reporting indicator = true
9. Account moves to a Final Status attribute
H Notes: Implemented in trigger rules
I Logic: Triggered when account status / related final-status fields satisfy final status trigger condition
10. Account deleted
H Notes: Implemented as exclusion / status-based logic
I Logic: Excluded when account status indicates deleted
11. Account is Reactivated
H Notes: Implemented in trigger/exclusion rules
I Logic: Triggered or excluded based on reactivation status and prior reported state
12. Account under non-605B Fraud Investigation
H Notes: Implemented in trigger rules
I Logic: Triggered when fraud claimed on account and not blocked condition is met
13. Account is manually suppressed
H Notes: Implemented in exclusion rules
I Logic: Excluded when credit bureau manual suppression indicator = true
14. Account is in a final status
H Notes: Implemented in exclusion logic
I Logic: Excluded based on final account status condition
15. Account is <30 days old
H Notes: Implemented in exclusion rules
I Logic: Excluded when account_open_date is within 30 days of current date
16. Account has already reported under non-605B Fraud Investigation
H Notes: Implemented in exclusion rules
I Logic: Excluded when prior reported fraud status already exists
17. Account under 605B Fraud Investigation
H Notes: Implemented in exclusion rules
I Logic: Excluded when fraud claimed and blocked condition is met
18. Account has already reported deleted
H Notes: Implemented in exclusion rules
I Logic: Excluded when previously reported deleted status is present
19. Account has already reported Reactivated
H Notes: Implemented in exclusion rules
I Logic: Excluded when prior reactivation has already been reported
20. Account fails eDQ Check
H Notes: EDQ work completed in CT4018T-491
I Logic: Final EDQ suppression applied by left anti join on account_id
