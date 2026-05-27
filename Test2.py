Secondary Consolidator Output validated — 61,727 distinct account_ids, all with sor_id = 6. PASS.

Run ID: card_recoveries_cdq780-recoveries-cte-05262026_c55840fe_API_ADHOC
Validated on: 2026-05-27

WITH src AS (
    SELECT account_id, sor_id
    FROM US_CARD.FRAUD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_CHARGEDOFF_ACCOUNTS_SECONDARY_CUSTOMERS_PHDP_CARD_VW
    WHERE run_id = 'card_recoveries_cdq780-recoveries-cte-05262026_c55840fe_API_ADHOC'
)
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT account_id) AS distinct_account_ids,
    COUNT(*) - COUNT(DISTINCT account_id) AS duplicate_account_id_rows,
    SUM(CASE WHEN sor_id = 6 THEN 1 ELSE 0 END) AS sor6_rows,
    COUNT(DISTINCT CASE WHEN sor_id = 6 THEN account_id END) AS unique_sor6_accounts,
    SUM(CASE WHEN sor_id <> 6 OR sor_id IS NULL THEN 1 ELSE 0 END) AS other_sor_rows,
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT account_id)
         AND SUM(CASE WHEN sor_id <> 6 OR sor_id IS NULL THEN 1 ELSE 0 END) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status
FROM src;

Result: total_rows=61727, distinct_account_ids=61727, duplicate_rows=0, sor6_rows=61727, other_sor_rows=0, validation_status=PASS
