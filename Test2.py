^([0-9a-zA-Z]( *[0-9a-zA-Z])*)?$



^[a-zA-Z]([ '-]*[a-zA-Z])*$
           ^[\x20-\x7E]*$

           ad zip postal code  ^[A-Za-z0-9 -]{3,10}$


-- ============================================================
-- CONFIG
-- ============================================================
SET run_id = '<your_run_id>';
SET tbl_transactions = '<DB.SCHEMA.CREDIT_CARD_TRANSACTION_AND_FINANCIAL_LEDGER>';
SET tbl_output       = '<DB.SCHEMA.latest_payment_output_or_final_table>';  -- where the function's result lands

-- ============================================================
-- 1. Replicate the function logic: latest PAYMENT date per account+sor
-- ============================================================
WITH expected AS (
  SELECT
    account_id,
    sor_id,
    MAX(transaction_effective_date) AS transaction_effective_date
  FROM IDENTIFIER($tbl_transactions)
  WHERE run_id = $run_id
    AND credit_card_transaction_category_class = 'PAYMENT'
  GROUP BY account_id, sor_id
)
SELECT * FROM expected
ORDER BY account_id, sor_id;
