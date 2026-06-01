-- ============================================================
-- CONFIG
-- ============================================================
SET run_id   = 'c897ba20-33bd-4d7b-994c-f23e4570272f-DR-04-26-dfsl1-test5';  -- calculator/reportable key
SET instnc   = '20260426';   -- INSTNC_ID on the RCVRY_CBR_SRVC_* source tables (confirm value)

SET tbl_calculator  = 'US_CARD.US_CARD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_CALCULATED_ACCOUNTS_CUSTOMERS_QA_V15_QHDP_CARD_NPI_VW';
SET tbl_reportable  = 'US_CARD.US_CARD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_METRO2_REPORTABLE_ACCOUNTS_QA_V10_QHDP_CARD_NPI_VW';
SET tbl_cb_account  = 'CARD_DB.QHDP_CARD.RCVRY_CBR_SRVC_CREDIT_BUREAU_ACCOUNT_os';
SET tbl_rpt_ovrd    = 'CARD_DB.QHDP_CARD.RCVRY_CBR_SRVC_REPORTING_OVERRIDE_os';
SET tbl_cb_customer = 'CARD_DB.QHDP_CARD.RCVRY_CBR_SRVC_CREDIT_BUREAU_CUSTOMER_os';

-- ============================================================
-- VALIDATION
-- account key on calculator/reportable = CONSUMER_ACCOUNT_NUMBER
-- account key on source tables         = ACCOUNT_ID  (<-- change here if it's CONSUMER_ACCOUNT_NUMBER)
-- ============================================================
WITH suppressed AS (      -- the 59 distinct excluded accounts
  SELECT DISTINCT c.CONSUMER_ACCOUNT_NUMBER AS account_key,
                  c.account_status
  FROM   IDENTIFIER($tbl_calculator) c
  WHERE  c.run_id = $run_id
    AND NOT EXISTS (
        SELECT 1 FROM IDENTIFIER($tbl_reportable) r
        WHERE r.run_id = $run_id
          AND r.CONSUMER_ACCOUNT_NUMBER = c.CONSUMER_ACCOUNT_NUMBER)
),
c1 AS (   -- C1: account reporting_status = 'S'
  SELECT DISTINCT ACCOUNT_ID AS account_key
  FROM   IDENTIFIER($tbl_cb_account)
  WHERE  INSTNC_ID = $instnc AND REPORTING_STATUS = 'S'
),
c2 AS (   -- C2: active override (closed_date IS NULL)
  SELECT DISTINCT ACCOUNT_ID AS account_key
  FROM   IDENTIFIER($tbl_rpt_ovrd)
  WHERE  INSTNC_ID = $instnc AND CLOSED_DATE IS NULL
),
c3 AS (   -- C3: ALL customers on the account have reporting_status = 'S'
  SELECT ACCOUNT_ID AS account_key
  FROM   IDENTIFIER($tbl_cb_customer)
  WHERE  INSTNC_ID = $instnc
  GROUP BY ACCOUNT_ID
  HAVING MAX(CASE WHEN REPORTING_STATUS = 'S' THEN 0 ELSE 1 END) = 0
)
SELECT
  s.account_key,
  s.account_status,
  CASE WHEN c1.account_key IS NOT NULL THEN 'Y' ELSE 'N' END AS hit_c1,
  CASE WHEN c2.account_key IS NOT NULL THEN 'Y' ELSE 'N' END AS hit_c2,
  CASE WHEN c3.account_key IS NOT NULL AND s.account_status <> 'DA' THEN 'Y' ELSE 'N' END AS hit_c3,
  CASE WHEN c1.account_key IS NOT NULL
         OR c2.account_key IS NOT NULL
         OR (c3.account_key IS NOT NULL AND s.account_status <> 'DA')
       THEN 'EXPLAINED' ELSE 'UNEXPLAINED_check_C4' END AS verdict
FROM suppressed s
LEFT JOIN c1 ON s.account_key = c1.account_key
LEFT JOIN c2 ON s.account_key = c2.account_key
LEFT JOIN c3 ON s.account_key = c3.account_key
ORDER BY verdict DESC, s.account_key;
