
Q10: SBFE - Out of 44 million accounts, only 30 are Small Business. Is the segment name correctly mapped? And all are excluded. Are all of them EDQ failures?

Run ID: card_recoveries_cdq780-recoveries-cte-05262026_c55840fe_API_ADHOC
Validated: 2026-05-27
Validated By: Amit

RESULT: PASS — Segment mapping correct, all 30 SBFE accounts excluded, all due to EDQ failures.

VALIDATION QUERY 1 — Confirm SBFE count and segment name in consolidator:

SELECT COUNT(*)
FROM US_CARD.FRAUD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_CONSOLIDATED_CHARGEDOFF_ACCOUNTS_CUSTOMERS_V2_PHDP_CARD_VW
WHERE run_id = 'card_recoveries_cdq780-recoveries-cte-05262026_c55840fe_API_ADHOC'
  AND segment_name = 'small_business';

Result: 30 accounts — matches the expected SBFE count. Segment name 'small_business' is correctly mapped.


VALIDATION QUERY 2 — Confirm all 30 SBFE accounts are excluded AND due to EDQ:

WITH sbfe AS (
    SELECT DISTINCT account_id
    FROM US_CARD.FRAUD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_CONSOLIDATED_CHARGEDOFF_ACCOUNTS_CUSTOMERS_V2_PHDP_CARD_VW
    WHERE run_id = 'card_recoveries_cdq780-recoveries-cte-05262026_c55840fe_API_ADHOC'
      AND segment_name = 'small_business'
),
edq_suppressed AS (
    SELECT DISTINCT account_id
    FROM US_CARD.FRAUD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_RECOVERIES_EDQ_SUPPRESSIONS_PHDP_CARD_VW
    WHERE run_id = 'card_recoveries_cdq780-recoveries-cte-05262026_c55840fe_API_ADHOC'
)
SELECT
    COUNT(*) AS total_sbfe,
    SUM(CASE WHEN ed.account_id IS NOT NULL THEN 1 ELSE 0 END) AS sbfe_edq_suppressed,
    SUM(CASE WHEN ed.account_id IS NULL THEN 1 ELSE 0 END) AS sbfe_NOT_edq
FROM sbfe s
LEFT JOIN edq_suppressed ed ON s.account_id = ed.account_id;

Result: total_sbfe=30, sbfe_edq_suppressed=30, sbfe_NOT_edq=0 — All 30 SBFE accounts are EDQ-suppressed (30/30 EDQ failure rate).

FINDING:
- Segment name 'small_business' is correctly mapped in the consolidator output
- All 30 SBFE accounts are excluded from the recovery pipeline (100%)
- All 30 exclusions are due to EDQ failures (no business-reason exclusions for SBFE)
- SBFE segment behavior is consistent with expectations

Snowflake Query ID: <paste from query history>
