-- Q11: Which fields have the most EDQ failures (suppressed=true) in calculator?
SELECT
    data_quality_input_field,
    COUNT(*)                          AS failure_count,
    COUNT(DISTINCT account_id)        AS distinct_accounts_failed
FROM US_CARD.FRAUD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_RECOVERIES_DATA_QUALITY_RESULTS_PHDP_CARD_VW
WHERE run_id = 'card_recoveries_cdq780-recoveries-cte-05262026_c55840fe_API_ADHOC'
  AND ecbr_suppressing_component = 'calculator'
  AND credit_bureau_reporting_suppression_status = TRUE
GROUP BY data_quality_input_field
ORDER BY failure_count DESC;


-- Q11 detailed: Which field + value + rule combinations are failing most?
SELECT
    data_quality_input_field,
    data_quality_input_value,
    data_quality_rule_id,
    COUNT(*)                          AS failure_count,
    COUNT(DISTINCT account_id)        AS distinct_accounts
FROM US_CARD.FRAUD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_RECOVERIES_DATA_QUALITY_RESULTS_PHDP_CARD_VW
WHERE run_id = 'card_recoveries_cdq780-recoveries-cte-05262026_c55840fe_API_ADHOC'
  AND ecbr_suppressing_component = 'calculator'
  AND credit_bureau_reporting_suppression_status = TRUE
GROUP BY data_quality_input_field, data_quality_input_value, data_quality_rule_id
ORDER BY failure_count DESC
LIMIT 50;



-- Q11 bonus: For each field, what % of records actually failed (suppressed)?
SELECT
    data_quality_input_field,
    COUNT(*)                                                                              AS total_evaluated,
    SUM(CASE WHEN credit_bureau_reporting_suppression_status = TRUE THEN 1 ELSE 0 END)    AS failures,
    SUM(CASE WHEN credit_bureau_reporting_suppression_status = FALSE THEN 1 ELSE 0 END)   AS passes,
    ROUND(100.0 * SUM(CASE WHEN credit_bureau_reporting_suppression_status = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_rate_pct
FROM US_CARD.FRAUD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_RECOVERIES_DATA_QUALITY_RESULTS_PHDP_CARD_VW
WHERE run_id = 'card_recoveries_cdq780-recoveries-cte-05262026_c55840fe_API_ADHOC'
  AND ecbr_suppressing_component = 'calculator'
GROUP BY data_quality_input_field
ORDER BY failures DESC;

