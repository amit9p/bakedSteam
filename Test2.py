
Q9: Excluded accounts are 22,144,882. What type of exclusion reasons are there for these? Are all of them EDQ suppressions?

Run ID: card_recoveries_cdq780-recoveries-cte-05262026_c55840fe_API_ADHOC
Validated: 2026-05-27
Validated By: Amit

RESULT: FAIL — Exclusions are NOT exclusively EDQ. Multiple non-EDQ business exclusion reasons identified.

VALIDATION QUERY — Exclusion reason breakdown (FLATTEN of EXCLUDED_REASON array):

SELECT
    reason.value::STRING AS exclusion_reason,
    COUNT(DISTINCT e.account_id) AS account_count
FROM US_CARD.FRAUD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_RECOVERIES_EXCLUDED_ACCOUNTS_PHDP_CARD_VW e,
LATERAL FLATTEN(input => e.excluded_reason) reason
WHERE e.run_id = 'card_recoveries_cdq780-recoveries-cte-05262026_c55840fe_API_ADHOC'
GROUP BY reason.value::STRING
ORDER BY account_count DESC;

Result — 12 distinct non-EDQ exclusion reasons identified:

Exclusion Reason | Account Count
SIF | 143,737
PIF | 143,737
DECEASED | 82,932
BANKRUPTCY | 72,400
605B | 61,674
NON_605B | 55,357
DELETED | 53,963
MANUAL_SUPPRESS | 35,572
AGED_DEBT | 12,067
PRE_CO_SIF | 6,161
TERMINAL | 5,067
REACTIVATED | 536
(other) | 33

Total accounts with non-EDQ reasons: ~673,236
Total excluded accounts: 22,144,888
Remaining (~21.47M): Excluded with empty EXCLUDED_REASON array — likely EDQ-suppressed accounts

FINDING:
- Exclusions come from MULTIPLE paths, not just EDQ
- ~673K accounts (~3%) are excluded for business reasons captured in EXCLUDED_REASON array
- ~21.47M accounts (~97%) have empty EXCLUDED_REASON arrays — these are presumed EDQ suppressions (to confirm)
- 12 distinct non-EDQ exclusion reasons exist: SIF, PIF, DECEASED, BANKRUPTCY, 605B, NON_605B, DELETED, MANUAL_SUPPRESS, AGED_DEBT, PRE_CO_SIF, TERMINAL, REACTIVATED
- Conclusion: The premise of Q9 ("Are ALL exclusions EDQ?") is FALSE. Exclusions are a UNION of EDQ suppressions + business suppressions

ACTION ITEMS:
- Confirm with Sakunthala/Mohan: are the ~21.47M empty-reason exclusions all EDQ-driven?
- Confirm with business: are the 12 non-EDQ reasons (SIF, PIF, DECEASED, etc.) expected in the recovery pipeline?
- Update design doc: document that EXCLUDED_ACCOUNTS = EDQ_SUPPRESSIONS ∪ BUSINESS_SUPPRESSIONS
- Consider: should EDQ accounts also have a reason tag (e.g., "EDQ") in EXCLUDED_REASON for traceability?

Snowflake Query ID: <paste from query history>



----------------

-- Q9 follow-up: What are the EXCLUSION REASONS for accounts NOT in EDQ suppressions?
WITH non_edq_excluded AS (
    SELECT account_id, excluded_reason
    FROM US_CARD.FRAUD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_RECOVERIES_EXCLUDED_ACCOUNTS_PHDP_CARD_VW
    WHERE run_id = 'card_recoveries_cdq780-recoveries-cte-05262026_c55840fe_API_ADHOC'
      AND account_id NOT IN (
            SELECT account_id
            FROM US_CARD.FRAUD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_RECOVERIES_EDQ_SUPPRESSIONS_PHDP_CARD_VW
            WHERE run_id = 'card_recoveries_cdq780-recoveries-cte-05262026_c55840fe_API_ADHOC'
      )
)
SELECT
    reason.value::STRING AS exclusion_reason,
    COUNT(DISTINCT n.account_id) AS account_count
FROM non_edq_excluded n,
LATERAL FLATTEN(input => n.excluded_reason) reason
GROUP BY reason.value::STRING
ORDER BY account_count DESC;





-- Q9: Are ALL excluded accounts from EDQ suppressions?
WITH excluded AS (
    SELECT DISTINCT account_id
    FROM US_CARD.FRAUD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_RECOVERIES_EXCLUDED_ACCOUNTS_PHDP_CARD_VW
    WHERE run_id = 'card_recoveries_cdq780-recoveries-cte-05262026_c55840fe_API_ADHOC'
),
edq_suppressed AS (
    SELECT DISTINCT account_id
    FROM US_CARD.FRAUD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_RECOVERIES_EDQ_SUPPRESSIONS_PHDP_CARD_VW
    WHERE run_id = 'card_recoveries_cdq780-recoveries-cte-05262026_c55840fe_API_ADHOC'
)
SELECT
    COUNT(*)                                                  AS total_excluded,
    SUM(CASE WHEN e.account_id IS NOT NULL THEN 1 ELSE 0 END) AS excluded_due_to_edq,
    SUM(CASE WHEN e.account_id IS NULL THEN 1 ELSE 0 END)     AS excluded_non_edq,
    ROUND(100.0 * SUM(CASE WHEN e.account_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_edq,
    CASE
        WHEN SUM(CASE WHEN e.account_id IS NULL THEN 1 ELSE 0 END) = 0
        THEN 'PASS - All exclusions are EDQ suppressions'
        ELSE 'FAIL - Some exclusions are NOT from EDQ'
    END AS validation_status
FROM excluded x
LEFT JOIN edq_suppressed e ON x.account_id = e.account_id;
