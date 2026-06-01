-- ================================================================
-- verify_reportable_exclusions.sql
-- Verifies why accounts in CALCULATOR are missing from REPORTABLE
-- (generator output), and whether the exclusions are legitimate
-- C1/C2/C3/C4 suppressions or a real code/data issue.
--
-- Rules from ecbr_generator/reportable_accounts.py:
--   C1: REPORTING_STATUS = 'S'                          (credit_bureau_account)
--   C2: active override -> CLOSED_DATE IS NULL          (reporting_override)
--   C3: ALL customers REPORTING_STATUS='S' AND status<>'DA' (credit_bureau_customer)
--   C4: account_id IN MANUALLY_SUPPRESSED_ACCOUNT_IDS   (hardcoded: 2 ids)
--
-- Account key = INTERNAL account_id (e.g. 20051066428), NOT the
-- 16-digit CONSUMER_ACCOUNT_NUMBER. Confirm the key column below.
-- Run the whole worksheet top to bottom; read each result grid.
-- ================================================================

-- ============================================================
-- CONFIG  -- edit these to match your environment
-- ============================================================
SET run_id = 'c897ba20-33bd-4d7b-994c-f23e4570272f-DR-04-26-dfsl1-test5';
SET instnc = '20260426';   -- INSTNC_ID on the RCVRY_CBR_SRVC_* source tables

SET tbl_calculator  = 'US_CARD.US_CARD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_CALCULATED_ACCOUNTS_CUSTOMERS_QA_V15_QHDP_CARD_NPI_VW';
SET tbl_reportable  = 'US_CARD.US_CARD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_METRO2_REPORTABLE_ACCOUNTS_QA_V10_QHDP_CARD_NPI_VW';
SET tbl_cb_account  = 'CARD_DB.QHDP_CARD.RCVRY_CBR_SRVC_CREDIT_BUREAU_ACCOUNT_os';
SET tbl_rpt_ovrd    = 'CARD_DB.QHDP_CARD.RCVRY_CBR_SRVC_REPORTING_OVERRIDE_os';
SET tbl_cb_customer = 'CARD_DB.QHDP_CARD.RCVRY_CBR_SRVC_CREDIT_BUREAU_CUSTOMER_os';

-- NOTE on the account key:
-- The C4 list values (20051066428) are the INTERNAL account_id, 11 digits,
-- which matches the source tables' ACCOUNT_ID (NUMBER). So below we join on
-- ACCOUNT_ID on every table. If your calculator/reportable views name the
-- internal id differently, find-and-replace ACCOUNT_ID in the calc/reportable
-- references only. The two manual ids are inlined where C4 is checked.


-- ============================================================
-- STEP 0  Count reconciliation (rows vs distinct accounts)
-- Expect: row diff = 63, distinct-account diff = 59 (4 dupes)
-- ============================================================
SELECT
  (SELECT COUNT(*) FROM IDENTIFIER($tbl_calculator) WHERE run_id=$run_id)                         AS calc_rows,
  (SELECT COUNT(DISTINCT ACCOUNT_ID) FROM IDENTIFIER($tbl_calculator) WHERE run_id=$run_id)       AS calc_distinct,
  (SELECT COUNT(*) FROM IDENTIFIER($tbl_reportable) WHERE run_id=$run_id)                         AS rep_rows,
  (SELECT COUNT(DISTINCT ACCOUNT_ID) FROM IDENTIFIER($tbl_reportable) WHERE run_id=$run_id)       AS rep_distinct,
  (SELECT COUNT(*) FROM IDENTIFIER($tbl_calculator) WHERE run_id=$run_id)
    - (SELECT COUNT(*) FROM IDENTIFIER($tbl_reportable) WHERE run_id=$run_id)                     AS row_diff,
  (SELECT COUNT(DISTINCT ACCOUNT_ID) FROM IDENTIFIER($tbl_calculator) WHERE run_id=$run_id)
    - (SELECT COUNT(DISTINCT ACCOUNT_ID) FROM IDENTIFIER($tbl_reportable) WHERE run_id=$run_id)   AS distinct_acct_diff;


-- ============================================================
-- STEP 1  Join-key sanity. Does the source ACCOUNT_ID overlap
-- the calculator ACCOUNT_ID at all? If this is 0 -> wrong key,
-- everything below is meaningless until fixed.
-- ============================================================
SELECT COUNT(*) AS source_rows_matching_calc_key
FROM IDENTIFIER($tbl_cb_account) a
WHERE a.INSTNC_ID = $instnc
  AND TO_VARCHAR(a.ACCOUNT_ID) IN (
      SELECT TO_VARCHAR(ACCOUNT_ID) FROM IDENTIFIER($tbl_calculator) WHERE run_id=$run_id);


-- ============================================================
-- STEP 2  Known C4 accounts must be suppressed:
-- in calculator, absent from reportable.
--   in_calc>0 & in_reportable=0 -> generator C4 works
--   in_reportable>0             -> BUG (manual id leaked through)
--   in_calc=0                   -> not in this run's data (coverage)
-- ============================================================
SELECT '20051066428' AS acct,
  (SELECT COUNT(*) FROM IDENTIFIER($tbl_calculator) WHERE run_id=$run_id AND TO_VARCHAR(ACCOUNT_ID)='20051066428') AS in_calc,
  (SELECT COUNT(*) FROM IDENTIFIER($tbl_reportable) WHERE run_id=$run_id AND TO_VARCHAR(ACCOUNT_ID)='20051066428') AS in_reportable
UNION ALL
SELECT '20049823958',
  (SELECT COUNT(*) FROM IDENTIFIER($tbl_calculator) WHERE run_id=$run_id AND TO_VARCHAR(ACCOUNT_ID)='20049823958'),
  (SELECT COUNT(*) FROM IDENTIFIER($tbl_reportable) WHERE run_id=$run_id AND TO_VARCHAR(ACCOUNT_ID)='20049823958');


-- ============================================================
-- STEP 3  Summary: tag every excluded account by C1/C2/C3/C4.
-- PASS when explained = total_suppressed and unexplained = 0.
-- ============================================================
WITH suppressed AS (
  SELECT DISTINCT c.ACCOUNT_ID AS account_key, c.ACCOUNT_STATUS AS account_status
  FROM IDENTIFIER($tbl_calculator) c
  WHERE c.run_id=$run_id
    AND NOT EXISTS (SELECT 1 FROM IDENTIFIER($tbl_reportable) r
                    WHERE r.run_id=$run_id AND r.ACCOUNT_ID=c.ACCOUNT_ID)
),
c1 AS (SELECT DISTINCT ACCOUNT_ID AS account_key FROM IDENTIFIER($tbl_cb_account)
       WHERE INSTNC_ID=$instnc AND REPORTING_STATUS='S'),
c2 AS (SELECT DISTINCT ACCOUNT_ID AS account_key FROM IDENTIFIER($tbl_rpt_ovrd)
       WHERE INSTNC_ID=$instnc AND CLOSED_DATE IS NULL),
c3 AS (SELECT ACCOUNT_ID AS account_key FROM IDENTIFIER($tbl_cb_customer)
       WHERE INSTNC_ID=$instnc GROUP BY ACCOUNT_ID
       HAVING MAX(CASE WHEN REPORTING_STATUS='S' THEN 0 ELSE 1 END)=0),
tagged AS (
  SELECT s.account_key,
    CASE WHEN c1.account_key IS NOT NULL THEN 1 ELSE 0 END AS h1,
    CASE WHEN c2.account_key IS NOT NULL THEN 1 ELSE 0 END AS h2,
    CASE WHEN c3.account_key IS NOT NULL AND s.account_status<>'DA' THEN 1 ELSE 0 END AS h3,
    CASE WHEN TO_VARCHAR(s.account_key) IN ('20051066428','20049823958') THEN 1 ELSE 0 END AS h4
  FROM suppressed s
  LEFT JOIN c1 ON s.account_key=c1.account_key
  LEFT JOIN c2 ON s.account_key=c2.account_key
  LEFT JOIN c3 ON s.account_key=c3.account_key
)
SELECT
  COUNT(*)                                            AS total_suppressed,
  SUM(h1)                                             AS by_c1_status_S,
  SUM(h2)                                             AS by_c2_active_override,
  SUM(h3)                                             AS by_c3_all_cust_S,
  SUM(h4)                                             AS by_c4_manual_list,
  SUM(CASE WHEN h1+h2+h3+h4>0 THEN 1 ELSE 0 END)      AS explained,
  SUM(CASE WHEN h1+h2+h3+h4=0 THEN 1 ELSE 0 END)      AS unexplained
FROM tagged;


-- ============================================================
-- STEP 4  Per-account detail. Run to see which rule caught each,
-- and to list any UNEXPLAINED accounts for code-vs-data review.
-- ============================================================
WITH suppressed AS (
  SELECT DISTINCT c.ACCOUNT_ID AS account_key, c.ACCOUNT_STATUS AS account_status
  FROM IDENTIFIER($tbl_calculator) c
  WHERE c.run_id=$run_id
    AND NOT EXISTS (SELECT 1 FROM IDENTIFIER($tbl_reportable) r
                    WHERE r.run_id=$run_id AND r.ACCOUNT_ID=c.ACCOUNT_ID)
),
c1 AS (SELECT DISTINCT ACCOUNT_ID AS account_key FROM IDENTIFIER($tbl_cb_account)
       WHERE INSTNC_ID=$instnc AND REPORTING_STATUS='S'),
c2 AS (SELECT DISTINCT ACCOUNT_ID AS account_key FROM IDENTIFIER($tbl_rpt_ovrd)
       WHERE INSTNC_ID=$instnc AND CLOSED_DATE IS NULL),
c3 AS (SELECT ACCOUNT_ID AS account_key FROM IDENTIFIER($tbl_cb_customer)
       WHERE INSTNC_ID=$instnc GROUP BY ACCOUNT_ID
       HAVING MAX(CASE WHEN REPORTING_STATUS='S' THEN 0 ELSE 1 END)=0)
SELECT
  s.account_key,
  s.account_status,
  CASE WHEN c1.account_key IS NOT NULL THEN 'Y' ELSE 'N' END AS hit_c1,
  CASE WHEN c2.account_key IS NOT NULL THEN 'Y' ELSE 'N' END AS hit_c2,
  CASE WHEN c3.account_key IS NOT NULL AND s.account_status<>'DA' THEN 'Y' ELSE 'N' END AS hit_c3,
  CASE WHEN TO_VARCHAR(s.account_key) IN ('20051066428','20049823958') THEN 'Y' ELSE 'N' END AS hit_c4,
  CASE WHEN c1.account_key IS NOT NULL OR c2.account_key IS NOT NULL
         OR (c3.account_key IS NOT NULL AND s.account_status<>'DA')
         OR TO_VARCHAR(s.account_key) IN ('20051066428','20049823958')
       THEN 'EXPLAINED' ELSE 'UNEXPLAINED' END AS verdict
FROM suppressed s
LEFT JOIN c1 ON s.account_key=c1.account_key
LEFT JOIN c2 ON s.account_key=c2.account_key
LEFT JOIN c3 ON s.account_key=c3.account_key
ORDER BY verdict DESC, s.account_key;


-- ============================================================
-- STEP 5  For one UNEXPLAINED account, dump its source rows to
-- decide CODE bug vs QA DATA issue. Replace <ID> with a value
-- from STEP 4's UNEXPLAINED rows, then run these three.
-- ============================================================
-- SELECT 'calculator'  AS src, * FROM IDENTIFIER($tbl_calculator)  WHERE run_id=$run_id AND ACCOUNT_ID=<ID>;
-- SELECT 'cb_account'   AS src, * FROM IDENTIFIER($tbl_cb_account)  WHERE INSTNC_ID=$instnc AND ACCOUNT_ID=<ID>;
-- SELECT 'cb_customer'  AS src, * FROM IDENTIFIER($tbl_cb_customer) WHERE INSTNC_ID=$instnc AND ACCOUNT_ID=<ID>;
-- SELECT 'rpt_override' AS src, * FROM IDENTIFIER($tbl_rpt_ovrd)    WHERE INSTNC_ID=$instnc AND ACCOUNT_ID=<ID>;
--
-- If source shows S / null-override / all-customers-S but our CTE missed it
--   -> CODE/logic gap (NULL handling, lowercase 's', whitespace, wrong join).
-- If source data is malformed (missing rows, unexpected nulls, e.g. the
--   account_type_code null pattern from run 20260205) -> QA DATA issue.
