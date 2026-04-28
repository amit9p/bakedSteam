
%sql

SELECT *
FROM parquet.`s3://my-bucket/data/cc_account/`
LIMIT 100;



Thanks Srinivas. I checked your query — the key validation there is the inner join between charged-off accounts and RA/customer data on "account_id + sor_id", which returned 22,426 distinct accounts. The left outer joins to estates and bankruptcy look like enrichment joins, so they should not reduce the distinct account count.

I’ll validate the same against the actual consolidator path in Databricks: "CCAccount + Fraud" left join "CustomerInformation" on "account_id + sor_id". I’ll compare total rows, matched customer records, and missing customer records, after aligning snapshot dates.

Yes, I’ll compare it against the actual consolidator join path. Your query is checking how many charged-off accounts have matching RA/customer records using an inner join on "account_id + sor_id".

In consolidator, the comparable step is "CCAccount + Fraud" left joined with "CustomerInformation" on "account_id + sor_id". I’ll run that join using the calculated temp views and capture total records, matched customer records, and missing customer records. I’ll also align the snapshot dates before comparing with the 22,426 count.



%sql

SELECT
    cc.*,
    f.is_identity_fraud_claimed_on_account,
    f.is_valid_identity_fraud_proven,
    f.identity_fraud_effective_date
FROM cc_account_calculated cc
LEFT JOIN fraud_calculated f
    ON cc.account_id = f.account_id
   AND cc.sor_id = f.sor_id;

____________
%sql

-- =========================================================
-- Fraud calculation SQL
-- Based on:
-- datasets/fraud/pre_process.py
-- constants.py
-- =========================================================

WITH constants AS (
    SELECT
        '6' AS OMEGA_SOR_ID,
        'RESOLVED' AS INCIDENT_STATUS_RESOLVED,
        'CONFIRMED_VALID' AS RESOLUTION_CODE_CONFIRMED_VALID,
        'FRAUD' AS FRAUD_INCIDENT_TYPE,
        'IDENTITY' AS IDENTITY_FRAUD_TYPE
),

-- =========================================================
-- 1. Read input dataset
-- From config.yaml:
-- incidents_service_incident_dmos
-- Replace table name below with your actual Databricks table/view
-- =========================================================
incidents_service_incident_dmos AS (
    SELECT *
    FROM your_catalog.your_schema.incidents_service_incident_dmos
),

-- =========================================================
-- 2. Filter only identity fraud incidents
-- PySpark equivalent:
-- incidents_df.filter(
--   col("incident_type") == FRAUD_INCIDENT_TYPE
--   & col("fraud_type") == IDENTITY_FRAUD_TYPE
-- )
-- =========================================================
identity_fraud_incidents AS (
    SELECT i.*
    FROM incidents_service_incident_dmos i
    CROSS JOIN constants c
    WHERE i.incident_type = c.FRAUD_INCIDENT_TYPE
      AND i.fraud_type = c.IDENTITY_FRAUD_TYPE
),

-- =========================================================
-- 3. Derive identity fraud flags per account_id
--
-- PySpark logic:
-- groupBy("account_id").agg(
--   count(when(incident_status != RESOLVED, 1)) > 0
--      AS is_identity_fraud_claimed_on_account,
--
--   count(when(
--       incident_status = RESOLVED
--       AND resolution_code = CONFIRMED_VALID, 1
--   )) > 0
--      AS is_valid_identity_fraud_proven,
--
--   max(date_entered)
--      AS identity_fraud_effective_date
-- )
-- =========================================================
identity_fraud_flags AS (
    SELECT
        CAST(i.account_id AS STRING) AS account_id,

        CASE
            WHEN COUNT(
                CASE
                    WHEN i.incident_status <> c.INCIDENT_STATUS_RESOLVED
                    THEN 1
                END
            ) > 0
            THEN true
            ELSE false
        END AS is_identity_fraud_claimed_on_account,

        CASE
            WHEN COUNT(
                CASE
                    WHEN i.incident_status = c.INCIDENT_STATUS_RESOLVED
                     AND i.resolution_code = c.RESOLUTION_CODE_CONFIRMED_VALID
                    THEN 1
                END
            ) > 0
            THEN true
            ELSE false
        END AS is_valid_identity_fraud_proven,

        MAX(CAST(i.date_entered AS DATE)) AS identity_fraud_effective_date

    FROM identity_fraud_incidents i
    CROSS JOIN constants c
    GROUP BY CAST(i.account_id AS STRING)
)

-- =========================================================
-- 4. Final fraud output frame
-- PySpark equivalent:
-- build_fraud_frame()
-- =========================================================
SELECT
    account_id,
    is_identity_fraud_claimed_on_account,
    is_valid_identity_fraud_proven,
    identity_fraud_effective_date,
    c.OMEGA_SOR_ID AS sor_id

FROM identity_fraud_flags f
CROSS JOIN constants c;

______

LEFT JOIN latest_payment_posted_dates lp
    ON CAST(a.account_id AS STRING) = CAST(lp.account_id AS STRING)
   AND CAST(a.sor_id AS STRING) = CAST(lp.sor_id AS STRING)

LEFT JOIN characteristics_flags cf
    ON CAST(a.account_id AS STRING) = CAST(cf.account_id AS STRING)
   AND CAST(a.sor_id AS STRING) = CAST(cf.sor_id AS STRING)

%sql

-- =========================================================
-- cc_account calculation SQL
-- Based on:
-- config.yaml -> cc_account input datasets
-- pre_process.py -> main() logic
-- constants.py -> constants/mappings
-- =========================================================

WITH constants AS (
    SELECT
        '6' AS OMEGA_SOR_ID,
        'SettlementPaid' AS SETTLEMENT_PAID,
        30 AS NEW_ACCOUNT_LOOKBACK_DAYS,
        10 AS DELINQUENCY_LOOKBACK_YEARS
),

-- =========================================================
-- 1. Input dataset 1:
-- credit_card_transaction_and_financial_ledger
-- =========================================================
credit_card_transaction_and_financial_ledger AS (
    SELECT *
    FROM your_catalog.your_schema.credit_card_transaction_and_financial_ledger
),

-- =========================================================
-- 2. Input dataset 2:
-- charged_off_credit_card_account_pt
-- =========================================================
charged_off_credit_card_account_pt AS (
    SELECT *
    FROM your_catalog.your_schema.charged_off_credit_card_account_pt
),

-- =========================================================
-- 3. Input dataset 3:
-- characteristics_service_characteristics
-- Config has date_range ["20260111", "20260111"]
-- Uncomment correct date filter if available in your table
-- =========================================================
characteristics_service_characteristics AS (
    SELECT *
    FROM your_catalog.your_schema.characteristics_service_characteristics
    -- WHERE business_date = '20260111'
    -- WHERE snapshot_date = DATE '2026-01-11'
),

-- =========================================================
-- 4. Apply exclusion rules on base account dataset
-- This happens BEFORE joins in PySpark code
-- =========================================================
account_snapshot_filtered AS (
    SELECT a.*
    FROM charged_off_credit_card_account_pt a
    CROSS JOIN constants c
    WHERE COALESCE(a.is_live_test_account, false) = false

      AND datediff(current_date(), CAST(a.account_open_date AS DATE))
            >= c.NEW_ACCOUNT_LOOKBACK_DAYS

      AND COALESCE(a.account_alpha_2_country_code, '') <> 'CA'

      AND (
            a.most_recent_delinquency_start_date IS NULL
            OR CAST(a.most_recent_delinquency_start_date AS DATE)
                >= add_months(
                       current_date(),
                       -1 * c.DELINQUENCY_LOOKBACK_YEARS * 12
                   )
          )
),

-- =========================================================
-- 5. Convert characteristics rows into flags/dates
-- Based on CHARACTERISTIC_MAPPING
-- TERMINAL     -> is_account_terminal, terminal_effective_date
-- SETTLED      -> is_account_settled, sif_effective_date
-- AGED_DEBT    -> is_account_aged_debt, aged_debt_effective_date
-- PAID_IN_FULL -> is_account_paid_in_full, pif_effective_date
-- =========================================================
characteristics_flags AS (
    SELECT
        ch.account_id,
        c.OMEGA_SOR_ID AS sor_id,

        MAX(CASE WHEN ch.characteristic = 'TERMINAL'
                 THEN true ELSE false END) AS is_account_terminal,

        MAX(CASE WHEN ch.characteristic = 'TERMINAL'
                 THEN CAST(ch.characteristic_entered_date AS DATE) END) AS terminal_effective_date,

        MAX(CASE WHEN ch.characteristic = 'SETTLED'
                 THEN true ELSE false END) AS is_account_settled,

        MAX(CASE WHEN ch.characteristic = 'SETTLED'
                 THEN CAST(ch.characteristic_entered_date AS DATE) END) AS sif_effective_date,

        MAX(CASE WHEN ch.characteristic = 'AGED_DEBT'
                 THEN true ELSE false END) AS is_account_aged_debt,

        MAX(CASE WHEN ch.characteristic = 'AGED_DEBT'
                 THEN CAST(ch.characteristic_entered_date AS DATE) END) AS aged_debt_effective_date,

        MAX(CASE WHEN ch.characteristic = 'PAID_IN_FULL'
                 THEN true ELSE false END) AS is_account_paid_in_full,

        MAX(CASE WHEN ch.characteristic = 'PAID_IN_FULL'
                 THEN CAST(ch.characteristic_entered_date AS DATE) END) AS pif_effective_date

    FROM characteristics_service_characteristics ch
    CROSS JOIN constants c
    GROUP BY
        ch.account_id,
        c.OMEGA_SOR_ID
),

-- =========================================================
-- 6. Get latest PAYMENT transaction date
-- PySpark:
-- transactions.filter(category = 'PAYMENT')
--             .groupBy(account_id, sor_id)
--             .max(transaction_effective_date)
-- =========================================================
latest_payment_posted_dates AS (
    SELECT
        t.account_id,
        t.sor_id,
        MAX(CAST(t.transaction_effective_date AS DATE)) AS transaction_effective_date
    FROM credit_card_transaction_and_financial_ledger t
    WHERE t.credit_card_transaction_category_class = 'PAYMENT'
    GROUP BY
        t.account_id,
        t.sor_id
),

-- =========================================================
-- 7. Join account snapshot with latest payment and characteristics
-- =========================================================
cc_account_joined AS (
    SELECT
        a.*,

        lp.transaction_effective_date,

        COALESCE(cf.is_account_terminal, false) AS is_account_terminal,
        cf.terminal_effective_date,

        COALESCE(cf.is_account_settled, false) AS is_account_settled,
        cf.sif_effective_date,

        COALESCE(cf.is_account_aged_debt, false) AS is_account_aged_debt,
        cf.aged_debt_effective_date,

        COALESCE(cf.is_account_paid_in_full, false) AS is_account_paid_in_full,
        cf.pif_effective_date

    FROM account_snapshot_filtered a

    LEFT JOIN latest_payment_posted_dates lp
        ON a.account_id = lp.account_id
       AND a.sor_id = lp.sor_id

    LEFT JOIN characteristics_flags cf
        ON a.account_id = cf.account_id
       AND a.sor_id = cf.sor_id
),

-- =========================================================
-- 8. Settlement notification flags
-- Based on SETTLEMENT_PAID = 'SettlementPaid'
-- =========================================================
cc_account_with_settlement AS (
    SELECT
        j.*,

        CASE
            WHEN j.is_account_settled = true
             AND j.charged_off_status_reason <> c.SETTLEMENT_PAID
            THEN true
            ELSE false
        END AS post_charge_off_settled_in_full_notification,

        CASE
            WHEN j.is_account_settled = true
             AND j.charged_off_status_reason = c.SETTLEMENT_PAID
            THEN true
            ELSE false
        END AS pre_charge_off_settled_in_full_notification

    FROM cc_account_joined j
    CROSS JOIN constants c
),

-- =========================================================
-- 9. Account closure reason mapping
-- Based on constants.py
-- =========================================================
cc_account_with_closure_reason AS (
    SELECT
        s.*,

        CASE
            WHEN s.account_lifecycle_status_reason IN (
                'ClosedPerConsumerZeroBalance',
                'ClosedPerConsumerWithBalance'
            )
            THEN 'closed_by_consumer'

            WHEN s.account_lifecycle_status_reason IN (
                'ClosedByBankZeroBalance',
                'ClosedDueToFirstPartyFraud',
                'ClosedDueToAutomatedFeeRuleUp',
                'ClosedDueToExtremelyLowBalance',
                'ClosedByBankWithBalance'
            )
            THEN 'closed_by_grantor'

            ELSE NULL
        END AS account_closure_reason

    FROM cc_account_with_settlement s
)

-- =========================================================
-- 10. Final cc_account frame
-- Based on build_cc_account_frame()
-- =========================================================
SELECT
    account_id,

    CAST(account_open_date AS DATE) AS account_open_date,

    CAST(most_recent_delinquency_start_date AS DATE)
        AS most_recent_delinquency_start_date,

    CAST(posted_balance AS INT) AS posted_balance,

    CAST(ROUND(charged_off_balance) AS INT)
        AS charged_off_balance,

    is_debt_sold,

    CAST(ROUND(credit_limit) AS INT)
        AS credit_limit,

    CAST(ROUND(account_highest_lifetime_balance_amount) AS INT)
        AS account_highest_lifetime_balance_amount,

    CAST(charged_off_date AS DATE) AS charged_off_date,

    CAST(account_close_date AS DATE) AS account_close_date,

    transaction_effective_date,

    is_account_terminal,
    terminal_effective_date,

    is_account_aged_debt,
    aged_debt_effective_date,

    is_account_paid_in_full,
    pif_effective_date,

    is_account_settled,
    sif_effective_date,

    post_charge_off_settled_in_full_notification,
    pre_charge_off_settled_in_full_notification,

    financial_portfolio_id,
    has_no_preset_spending_limit,
    past_due_status_reason,

    account_lifecycle_status_code,
    account_lifecycle_status_reason,

    sor_id,
    is_live_test_account,
    reactivation_status,

    previous_sor_account_id,
    charged_off_status_reason,
    previous_sor_id,
    service_owner_code,

    account_closure_reason,

    false AS tsys_pre_co_suppression_indicator,
    '' AS tsys_pre_co_suppression_reason_code

FROM cc_account_with_closure_reason;
