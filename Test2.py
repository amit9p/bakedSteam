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
