-- Q11 summary: Top 5 failing fields only
SELECT
    data_quality_input_field,
    COUNT(*)                                                    AS total_failures,
    COUNT(DISTINCT account_id)                                  AS distinct_accounts,
    LISTAGG(DISTINCT data_quality_input_value, ' | ')
        WITHIN GROUP (ORDER BY data_quality_input_value)        AS sample_bad_values
FROM US_CARD.FRAUD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_RECOVERIES_DATA_QUALITY_RESULTS_PHDP_CARD_VW
WHERE run_id = 'card_recoveries_cdq780-recoveries-cte-05262026_c55840fe_API_ADHOC'
  AND ecbr_suppressing_component = 'calculator'
  AND credit_bureau_reporting_suppression_status = TRUE
GROUP BY data_quality_input_field
ORDER BY total_failures DESC
LIMIT 5;
