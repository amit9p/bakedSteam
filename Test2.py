
/* =============================================================================
   SNOWFLAKE MEDALLION PIPELINE  (BRONZE -> SILVER -> [GOLD])
   -----------------------------------------------------------------------------
   Scenario : 80 reports, each sourcing ~13 tables already in Snowflake (~1000+).
   Goal      : For every source table, build a clean, DEDUPED silver table,
               refreshed daily (truncate + reload with dedup).
   Layers    :
     BRONZE  = raw landed tables (already exist in Snowflake) - READ ONLY
     SILVER  = cleaned, deduped, 1:1 with bronze - rebuilt daily - CONSUMABLE
     GOLD    = (optional) business aggregates / report-ready joins
   Design    : 100% METADATA-DRIVEN. You never hand-write 1000 scripts.
               One config table + one procedure + tasks drive everything.
   Dedup rule: primary key + keep latest by timestamp (configurable per table).
   ============================================================================= */


/* -----------------------------------------------------------------------------
   STEP 0 — Database & schema layout (one-time)
   Use schemas to represent the medallion layers cleanly.
   ----------------------------------------------------------------------------- */
USE ROLE      SYSADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS REPORTING_DW;
USE DATABASE REPORTING_DW;

CREATE SCHEMA IF NOT EXISTS BRONZE;   -- raw source tables (already populated)
CREATE SCHEMA IF NOT EXISTS SILVER;   -- cleaned + deduped tables (built daily)
CREATE SCHEMA IF NOT EXISTS GOLD;     -- optional report-ready models
CREATE SCHEMA IF NOT EXISTS META;     -- pipeline control: config + logging


/* -----------------------------------------------------------------------------
   STEP 1 — The control/config table (the heart of the whole design)
   ONE ROW per source->silver table mapping. For 80 reports x 13 tables,
   this is ~1040 rows you manage here instead of 1040 scripts.
       report_id   : which report this table belongs to (lets you run per report)
       bronze_table: fully-qualified source table
       silver_table: target deduped table to (re)build
       key_cols    : business key defining a duplicate ('ID' or 'A_ID, B_ID')
       order_col   : recency column; latest row per key is kept
       is_active   : toggle a table on/off without deleting the row
   ----------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE META.TABLE_CONFIG (
    report_id     STRING,
    bronze_table  STRING,
    silver_table  STRING,
    key_cols      STRING,
    order_col     STRING,
    is_active     BOOLEAN DEFAULT TRUE,
    load_priority INT     DEFAULT 100   -- optional: control build order
);

-- Example rows for REPORT_01 (its 13 tables). Repeat the pattern for all reports.
INSERT INTO META.TABLE_CONFIG (report_id, bronze_table, silver_table, key_cols, order_col) VALUES
 ('REPORT_01', 'BRONZE.R01_CUSTOMER',     'SILVER.R01_CUSTOMER',     'CUSTOMER_ID',          'UPDATED_TS'),
 ('REPORT_01', 'BRONZE.R01_ACCOUNT',      'SILVER.R01_ACCOUNT',      'ACCOUNT_ID',           'UPDATED_TS'),
 ('REPORT_01', 'BRONZE.R01_TXN',          'SILVER.R01_TXN',          'TXN_ID',               'LOAD_TS'),
 ('REPORT_01', 'BRONZE.R01_ADDRESS',      'SILVER.R01_ADDRESS',      'ADDRESS_ID',           'UPDATED_TS'),
 ('REPORT_01', 'BRONZE.R01_PRODUCT',      'SILVER.R01_PRODUCT',      'PRODUCT_ID',           'UPDATED_TS');
 -- ... add the remaining 8 tables for REPORT_01, then REPORT_02's 13, etc.
 -- Tip: you can bulk-load this config from a CSV/spreadsheet instead of typing.


/* -----------------------------------------------------------------------------
   STEP 2 — A run-log table (so you have an audit trail for every refresh)
   Critical in a regulated/enterprise setting: who ran, when, rows, status.
   ----------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE META.RUN_LOG (
    run_ts        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    report_id     STRING,
    silver_table  STRING,
    source_rows   NUMBER,
    silver_rows   NUMBER,
    dupes_removed NUMBER,
    status        STRING,
    message       STRING
);


/* -----------------------------------------------------------------------------
   STEP 3 — The core SILVER build procedure (metadata-driven, handles all)
   For each active config row (optionally filtered by report_id):
     - rebuild the silver table from bronze, deduped via QUALIFY ROW_NUMBER
     - log source rows, silver rows, dupes removed, status
   CREATE OR REPLACE = clean daily full reload (your chosen strategy).
   ----------------------------------------------------------------------------- */
CREATE OR REPLACE PROCEDURE META.BUILD_SILVER(P_REPORT_ID STRING DEFAULT NULL)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  c CURSOR FOR
    SELECT report_id, bronze_table, silver_table, key_cols, order_col
    FROM META.TABLE_CONFIG
    WHERE is_active = TRUE
      AND (:P_REPORT_ID IS NULL OR report_id = :P_REPORT_ID)
    ORDER BY load_priority, report_id;
  v_src_rows  NUMBER;
  v_sil_rows  NUMBER;
  n_ok        INT DEFAULT 0;
  n_err       INT DEFAULT 0;
BEGIN
  FOR r IN c DO
    BEGIN
      -- 1) Rebuild silver table, deduped (latest row per key wins)
      EXECUTE IMMEDIATE
        'CREATE OR REPLACE TABLE ' || r.silver_table || ' AS ' ||
        'SELECT * FROM ' || r.bronze_table ||
        ' QUALIFY ROW_NUMBER() OVER (' ||
        '   PARTITION BY ' || r.key_cols ||
        '   ORDER BY ' || r.order_col || ' DESC) = 1';

      -- 2) Capture row counts for the log
      EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || r.bronze_table INTO v_src_rows;
      EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || r.silver_table INTO v_sil_rows;

      -- 3) Log success
      INSERT INTO META.RUN_LOG (report_id, silver_table, source_rows, silver_rows, dupes_removed, status, message)
      VALUES (r.report_id, r.silver_table, :v_src_rows, :v_sil_rows, :v_src_rows - :v_sil_rows, 'SUCCESS', NULL);

      n_ok := n_ok + 1;

    EXCEPTION
      WHEN OTHER THEN
        INSERT INTO META.RUN_LOG (report_id, silver_table, status, message)
        VALUES (r.report_id, r.silver_table, 'FAILED', SQLERRM);
        n_err := n_err + 1;
    END;
  END FOR;

  RETURN 'BUILD_SILVER done. Success=' || n_ok || ', Failed=' || n_err ||
         COALESCE(' (report=' || :P_REPORT_ID || ')', ' (all reports)');
END;
$$;


/* -----------------------------------------------------------------------------
   STEP 4 — Schedule it. Two common patterns — pick one.
   ----------------------------------------------------------------------------- */

-- PATTERN A: One daily task that rebuilds ALL reports' silver tables at once.
CREATE OR REPLACE TASK META.TSK_BUILD_SILVER_ALL
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = 'USING CRON 0 5 * * * America/Chicago'   -- 5 AM daily
AS
  CALL META.BUILD_SILVER(NULL);          -- NULL = all reports

ALTER TASK META.TSK_BUILD_SILVER_ALL RESUME;


-- PATTERN B (optional): One task PER report, if reports refresh on different
-- schedules or you want isolation. Example for REPORT_01:
-- CREATE OR REPLACE TASK META.TSK_BUILD_SILVER_R01
--   WAREHOUSE = COMPUTE_WH
--   SCHEDULE  = 'USING CRON 0 5 * * * America/Chicago'
-- AS
--   CALL META.BUILD_SILVER('REPORT_01');
-- ALTER TASK META.TSK_BUILD_SILVER_R01 RESUME;


/* -----------------------------------------------------------------------------
   STEP 5 — (OPTIONAL) GOLD layer: report-ready models
   Gold is where you JOIN the clean silver tables into the shape each report
   consumes. Since you said silver is already consumable, gold is optional.
   Build gold the same metadata way, OR as explicit views/tables per report.
   Example: a simple gold view for REPORT_01 joining 3 silver tables.
   ----------------------------------------------------------------------------- */
CREATE OR REPLACE VIEW GOLD.R01_CUSTOMER_360 AS
SELECT
    c.customer_id,
    c.customer_name,
    a.account_id,
    a.account_status,
    addr.city,
    addr.state
FROM SILVER.R01_CUSTOMER c
LEFT JOIN SILVER.R01_ACCOUNT a   ON a.customer_id = c.customer_id
LEFT JOIN SILVER.R01_ADDRESS addr ON addr.address_id = c.address_id;
-- Views auto-reflect the freshest silver data (no rebuild needed). Use a TABLE
-- + task instead if gold needs heavy precomputation.


/* -----------------------------------------------------------------------------
   STEP 6 — Operate, test & verify
   ----------------------------------------------------------------------------- */
-- Run everything now (don't wait for 5 AM):
CALL META.BUILD_SILVER(NULL);

-- Run just one report:
CALL META.BUILD_SILVER('REPORT_01');

-- Verify no duplicates in a silver table (expect 0 rows):
SELECT CUSTOMER_ID, COUNT(*) c
FROM SILVER.R01_CUSTOMER GROUP BY 1 HAVING COUNT(*) > 1;

-- Review the run log (counts, dupes removed, failures):
SELECT * FROM META.RUN_LOG ORDER BY run_ts DESC LIMIT 50;

-- See only failures from the latest run:
SELECT * FROM META.RUN_LOG WHERE status = 'FAILED' ORDER BY run_ts DESC;

-- Task history:
SELECT NAME, STATE, SCHEDULED_TIME, ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
ORDER BY SCHEDULED_TIME DESC LIMIT 20;


/* =============================================================================
   DESIGN NOTES & SCALING GUIDANCE
   - WHY metadata-driven: 80 reports x 13 tables = ~1040 tables. Hand-writing is
     unmaintainable. One config table + one proc scales to any number of tables.
   - Composite keys: put 'COL_A, COL_B' in key_cols; PARTITION BY handles it.
   - No timestamp on a table? Use a load-sequence/file-load-time column, or for
     full-row dedup PARTITION BY all columns.
   - Full reload vs incremental: full reload (CREATE OR REPLACE) is simplest and
     self-correcting. If some bronze tables are huge (50M+ rows) and reload gets
     slow/costly, switch THOSE to incremental using Streams on the bronze table
     + MERGE into silver. Keep the same config-driven structure.
   - Warehouse sizing: 1040 rebuilds in one task may need a larger warehouse or
     splitting into per-report tasks (Pattern B) that run in parallel. Start
     small, watch RUN_LOG timings, scale the warehouse or parallelize as needed.
   - Parallelism: Snowflake tasks run serially within one task. To parallelize,
     create multiple tasks (e.g., one per report or per group of reports) so
     they run concurrently on the warehouse.
   - Dependencies: if gold must run AFTER silver, chain with task AFTER:
       CREATE TASK META.TSK_BUILD_GOLD ... AFTER META.TSK_BUILD_SILVER_ALL AS ...
   - Idempotent: rerunning is safe — CREATE OR REPLACE always yields a clean table.
   - Lineage/observability: RUN_LOG gives you per-table source vs silver counts
     and dupes removed every day — great for monitoring + audits.
   ============================================================================= */
