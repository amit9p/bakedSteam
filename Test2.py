%sql
SELECT COUNT(DISTINCT A.account_id) AS reportable_accounts
FROM account_service_customer A
LEFT JOIN account_service_account B
  ON A.account_id = B.account_id
LEFT JOIN credit_bureau_reporting_service_credit_bureau_customer C
  ON A.account_id = C.account_id AND A.customer_id = C.customer_id
LEFT JOIN account_service_address D
  ON A.customer_id = D.customer_id
LEFT JOIN collector_service_account_link E
  ON A.account_id = E.account_id
LEFT JOIN collector_service_collector_configuration F
  ON E.collector_configuration_id = F.collector_configuration_id
LEFT JOIN credit_bureau_reporting_service_credit_bureau_account G
  ON C.account_id = G.account_id
LEFT ANTI JOIN enterprise_credit_bureau_reporting_card_metro2_reportable_accounts_base H
  ON A.account_id = H.account_id
WHERE A.cust_role_cd IN ('PRIMARY','SECONDARY')
