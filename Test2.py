SELECT DISTINCT
       o1.account_id
FROM US_CARD_CORE.CREDIT_CARD_TRANSACTIONS_AND_FINANCIAL_LEDGER_PARTICIPANTS_END_OF_DAY_PT_V2_QHDP_CARD_VW o1
INNER JOIN ENTERPRISE_SERVICES.ENTERPRISE_PRODUCT_AND_EXPERIENCE.ENTERPRISE_CUSTOMER_AND_REPORTED_ACCOUNT_FULL_FILE_V4_V11_QHDP_CUST_RA_NPI_VW o2
    ON o1.account_id = o2.account_id
WHERE o1.snapshot_date = '2026-04-22';



SELECT c.customer_id,
       c.customer_name,
       o.order_id,
       o.amount
FROM customers c
INNER JOIN orders o
    ON c.customer_id = o.customer_id;


Since the platform is calling this method positionally, "**kwargs" does not really help unless the platform also changes its code to pass named arguments. For the current issue, the guard-based fix makes more sense because it handles the existing platform behavior directly and gives us backward compatibility without requiring platform-side changes.



Hi all, I raised a PR for the backward-compatibility fix in "get_reportable_accounts".

This handles the case where "edq.suppressions" is not in config and "context" gets passed as the 3rd positional argument. I also added unit test coverage for it.

Please review when you get a chance. Thanks.



Updated `get_reportable_accounts` to support backward-compatible positional argument handling.

Issue:
When `edq.suppressions` is removed from config, the platform passes `context` as the 3rd positional argument. Because the current method signature expects `edq_suppressions_df` as the 3rd argument, the context dict gets bound there and causes `'dict' object has no attribute 'select'`.

Fix:
Added a guard inside `get_reportable_accounts` to detect this case:
- if the 3rd argument is a dict and `context` is `None`, treat it as `context`
- set `edq_suppressions_df` to `None`

Also added a unit test to validate this backward compatibility path.

This keeps existing behavior intact for valid EDQ suppression dataframe inputs while preventing runtime failure when config does not include EDQ suppression.




def test_context_passed_as_third_argument(self, calculated_df, consolidated_df):
    """
    Validates backward compatibility when context is passed as the
    3rd positional argument instead of edq_suppressions_df.
    """

    result_df = get_reportable_accounts(
        calculated_df,
        consolidated_df,
        CONSUMER_THIRD_THURSDAY_CONTEXT,
    )

    result_accounts = [row.account_id for row in result_df.select("account_id").collect()]

    assert result_df.count() > 0
    assert "1" in result_accounts



_____________


def get_reportable_accounts(
    calculated_dataset: DataFrame,
    consolidated_dataset: DataFrame,
    edq_suppressions_df: DataFrame = None,
    context: dict = None,
) -> DataFrame:
    """
    Args:
        calculated_dataset: DataFrame with calculated fields
        consolidated_dataset: DataFrame with consolidated fields
        edq_suppressions_df: DataFrame with edq suppression fields
        context: job_context dict from the platform framework (optional)
    """

    # Backward compatibility fix:
    # if framework passes context as 3rd positional argument,
    # it lands in edq_suppressions_df by mistake.
    if isinstance(edq_suppressions_df, dict) and context is None:
        context = edq_suppressions_df
        edq_suppressions_df = None

    # normal context handling
    if context is None:
        context = {}

    product_type = context.get("product_type", "consumer")
    reporting_date = context.get("reporting_date")

    if product_type.lower() == "consumer":
        override_rules_dict = CONSUMER_OVERRIDE_RULES
    elif product_type.lower() == "sbfe":
        override_rules_dict = SBFE_OVERRIDE_RULES
    else:
        logger.error(f"Invalid product_type: {product_type}")
        raise ValueError(f"Invalid product_type: {product_type}")

    spark = SparkSession.getActiveSession()

    # if EDQ df not passed, use empty df
    if edq_suppressions_df is None:
        edq_suppressions_df = spark.createDataFrame([], "account_id string")

    # rest of your existing code...


<><><><><><<






from pyspark.sql import functions as F

joiner_ids = joiner_output_df.select("account_id").dropDuplicates()
consolidator_ids = consolidator_output_df.select("account_id").dropDuplicates()
edq_ids = edq_suppressions_df.select("account_id").dropDuplicates()

base_reportable_ids = joiner_ids.join(consolidator_ids, on="account_id", how="inner")

print("base_reportable_ids:", base_reportable_ids.count())
print("edq_ids:", edq_ids.count())

matched_with_edq = base_reportable_ids.join(edq_ids, on="account_id", how="inner")
print("base ids matched in edq:", matched_with_edq.count())

remaining_after_edq = base_reportable_ids.join(edq_ids, on="account_id", how="left_anti")
print("remaining after edq:", remaining_after_edq.count())



Title
Create generator output testing strategy and readiness tracking for ECBR reportable account logic
Story / Background
As part of generator output readiness, we need a structured testing strategy for the reportable accounts logic so that all finalized intents can be validated in a consistent and traceable way.
This effort will use the Generator O/P Readiness sheet as the central tracker for different test scenarios, including single-rule cases, exclusion and override combinations, many-exclusion/many-override cases, EDQ failure scenarios, and report/do-not-report outcomes.
The goal is to ensure that finalized business intent is mapped to test coverage, test execution status, and implementation readiness, so the team has clear visibility into what is ready, what is pending, and what requires follow-up before E2E validation.
Scope
This story covers creation of the generator testing strategy and readiness process for:
single reporting and exclusion rules
one exclusion plus one override combinations
many exclusions with one override
one exclusion with many overrides
many exclusions with many overrides
EDQ failure / suppression-related scenarios
validation of expected report vs do-not-report behavior
linkage of each scenario to feature test availability, test execution, result tracking, and follow-up actions
Acceptance Criteria
A generator output test strategy is documented for all major scenario groups in the readiness sheet.
Each scenario clearly identifies expected outcome as report or do not report.
Finalized intents are marked and separated from non-finalized items.
Test coverage status is tracked for each scenario, including whether test is added to feature tests.
Execution fields are available for tested status, result, date tested, and tester name.
Follow-up items are captured for scenarios where intent is unclear, fields are unavailable, or implementation is pending.
Readiness can be determined using tracker columns such as intent finalized, implemented, all required fields available, and test execution result.
The tracker can be used as a one-stop reference during QA, dev sync, and E2E discussions.
Deliverables
Generator O/P Readiness sheet updated with scenario-based testing strategy
Mapping of scenarios to expected output behavior
Test readiness status for each scenario
Follow-up notes for pending intent / implementation gaps
Out of Scope
fixing generator code defects
changing business intent itself
downstream consolidator or calculator logic changes unless required for generator test input readiness
Definition of Done
test strategy is documented in the sheet
major generator scenarios are captured
expected outcomes are defined
pending vs ready scenarios are clearly visible
team can use the sheet for test execution planning and readiness review




Title
Create generator output testing strategy and readiness tracking for ECBR reportable account logic
Story / Background
As part of generator output readiness, we need a structured testing strategy for the reportable accounts logic so that all finalized intents can be validated in a consistent and traceable way.
This effort will use the Generator O/P Readiness sheet as the central tracker for different test scenarios, including single-rule cases, exclusion and override combinations, many-exclusion/many-override cases, EDQ failure scenarios, and report/do-not-report outcomes.
The goal is to ensure that finalized business intent is mapped to test coverage, test execution status, and implementation readiness, so the team has clear visibility into what is ready, what is pending, and what requires follow-up before E2E validation.
Scope
This story covers creation of the generator testing strategy and readiness process for:
single reporting and exclusion rules
one exclusion plus one override combinations
many exclusions with one override
one exclusion with many overrides
many exclusions with many overrides
EDQ failure / suppression-related scenarios
validation of expected report vs do-not-report behavior
linkage of each scenario to feature test availability, test execution, result tracking, and follow-up actions
Acceptance Criteria
A generator output test strategy is documented for all major scenario groups in the readiness sheet.
Each scenario clearly identifies expected outcome as report or do not report.
Finalized intents are marked and separated from non-finalized items.
Test coverage status is tracked for each scenario, including whether test is added to feature tests.
Execution fields are available for tested status, result, date tested, and tester name.
Follow-up items are captured for scenarios where intent is unclear, fields are unavailable, or implementation is pending.
Readiness can be determined using tracker columns such as intent finalized, implemented, all required fields available, and test execution result.
The tracker can be used as a one-stop reference during QA, dev sync, and E2E discussions.
Deliverables
Generator O/P Readiness sheet updated with scenario-based testing strategy
Mapping of scenarios to expected output behavior
Test readiness status for each scenario
Follow-up notes for pending intent / implementation gaps
Out of Scope
fixing generator code defects
changing business intent itself
downstream consolidator or calculator logic changes unless required for generator test input readiness
Definition of Done
test strategy is documented in the sheet
major generator scenarios are captured
expected outcomes are defined
pending vs ready scenarios are clearly visible
team can use the sheet for test execution planning and readiness review



________


my_data_df.coalesce(1).write.mode("overwrite").parquet(temp_output_path)


Reordered "get_reportable_accounts" parameters so all three DataFrame inputs are passed first ("calculated_dataset", "consolidated_dataset", "edq_suppressions_df") and "context" is passed last.

This change was made to avoid incorrect positional mapping, where the EDQ suppression DataFrame could be interpreted as "context". Since "context" is a config/runtime dictionary and not a DataFrame, that mismatch could cause runtime failures in Glue/job execution.

Also updated related call sites and tests to match the new method signature.

Resolution Answer → leave blank for now, or say
Current generator logic uses deceased indicator/status, not explicit date-based logic
Comments / Follow-up questions →
Current generator trigger appears to rely on is_account_holder_deceased / deceased status condition. No explicit logic found choosing between customer_deceased_date and deceased notification date. Business intent clarification still needed.
Current generator exclusion logic checks datediff(current_date(), account_open_date) < 30

Got it — I’ll go through the outstanding items tracker and focus on the generator-related open items first. I’ll update the sheet where I can confidently map things to current implementation and call out anything that still needs intent or upstream clarification.



A6 – I (Notes):
Dependent on generator run-date/context work. See CT4018T-525
A6 – J (Logic):
Third-Thursday condition uses generator date/context logic and should be finalized after CT4018T-525 is completed
For A7, you can keep:
I (Notes):
Awaiting final generator condition / tracking confirmation
J (Logic):
CCC-change condition tracking is still being confirmed in current generator implementation

_______

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
