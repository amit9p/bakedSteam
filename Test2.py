Triggered accounts = the input set pulled into the run (before suppression). Final reportable accounts = what survives the calculator + reportable suppression logic and actually gets reported. So Triggered ≥ Final reportable, and the gap is the suppressed/non-reportable accounts (the C1–C4 suppression rules). Lining up with our earlier run: calculator ~46,486 in vs ~46,423 reportable out, the diff being suppressed accounts.





The `ah_previous_account_number` rule is failing because the regex requires at least one alphanumeric char, but per the Confluence spec this field is blank fill (SBFE reads blank, no AH segment). Updating the pattern to make the value optional so blank passes:

`^([0-9a-zA-Z]( *[0-9a-zA-Z])*)?$`

Still accepts a valid alphanumeric value for the non-16-digit edge case, but allows blank too. Will make the change unless there are objections


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
