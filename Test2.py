WITH src AS (
    SELECT account_id, sor_id
    FROM US_CARD.FRAUD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_CHARGEDOFF_ACCOUNTS_SECONDARY_CUSTOMERS_PHDP_CARD_VW
    WHERE run_id = 'card_recoveries_cdq780-recoveries-cte-05262026_c55840fe_API_ADHOC'
)
SELECT
    COUNT(*)                                           AS total_rows,
    COUNT(DISTINCT account_id)                         AS distinct_account_ids,
    COUNT(*) - COUNT(DISTINCT account_id)              AS duplicate_count,
    SUM(CASE WHEN sor_id = 6 THEN 1 ELSE 0 END)        AS rows_with_sor_id_6,
    SUM(CASE WHEN sor_id <> 6 OR sor_id IS NULL THEN 1 ELSE 0 END) AS rows_with_other_sor_id,

    -- overall verdict
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT account_id)
         AND SUM(CASE WHEN sor_id = 6 THEN 1 ELSE 0 END) = COUNT(*)
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status
FROM src;




SELECT account_id, COUNT(*) AS dup_count
FROM US_CARD.FRAUD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_CHARGEDOFF_ACCOUNTS_SECONDARY_CUSTOMERS_PHDP_CARD_VW
WHERE run_id = 'card_recoveries_cdq780-recoveries-cte-05262026_c55840fe_API_ADHOC'
GROUP BY account_id
HAVING COUNT(*) > 1
ORDER BY dup_count DESC;
