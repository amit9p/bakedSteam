
/* =============================================================================
   SNOWFLAKE STAGING PIPELINE  —  Daily Full Reload + Dedup
   -----------------------------------------------------------------------------
   Use case : 13 source tables already in Snowflake contain duplicate rows.
   Goal      : 13 STAGING tables, no duplicates, fully rebuilt every day.
   Refresh   : FULL RELOAD daily (CREATE OR REPLACE = clean truncate+reload).
   Dedup rule: Primary key + keep the LATEST row by timestamp.
   Automation: One stored procedure + one scheduled TASK. Zero manual work.
   ============================================================================= */


/* -----------------------------------------------------------------------------
   STEP 0 — Context (edit names to match your environment)
   ----------------------------------------------------------------------------- */
USE ROLE      SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE  PHDP_CARD_PRTNR_NPI_BT;

CREATE SCHEMA IF NOT EXISTS STG;    -- clean, deduped staging tables live here
CREATE SCHEMA IF NOT EXISTS META;   -- pipeline config + procedure + task


/* -----------------------------------------------------------------------------
   STEP 1 — Config table: one row per source table.
   This is the ONLY thing you maintain. Add/remove tables here, not in code.
       key_cols  = primary/business key that defines a duplicate
       order_col = timestamp column; latest value is the row we keep
   For COMPOSITE keys, comma-separate: 'COL_A, COL_B'
   ----------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE META.DEDUP_CONFIG (
    src_table  STRING,   -- fully-qualified existing source table
    stg_table  STRING,   -- staging table to (re)build
    key_cols   STRING,   -- e.g. 'APPLICATION_ID'  (or 'A_ID, B_ID')
    order_col  STRING,   -- e.g. 'UPDATED_TS'  (latest wins)
    is_active  BOOLEAN DEFAULT TRUE
);

INSERT INTO META.DEDUP_CONFIG (src_table, stg_table, key_cols, order_col) VALUES
 ('APPTRACK_DBO_BT_APPLICATION',          'STG.BT_APPLICATION',          'APPLICATION_ID',         'UPDATED_TS'),
 ('APPTRACK_DBO_BT_APPLICATION_CONTACT',  'STG.BT_APPLICATION_CONTACT',  'APPLICATION_CONTACT_ID', 'UPDATED_TS'),
 ('APPTRACK_DBO_BT_CD_CONTACT_TYPE',      'STG.BT_CD_CONTACT_TYPE',      'CONTACT_TYPE_ID',        'UPDATED_TS'),
 ('APPTRACK_DBO_BT_CONTACT',              'STG.BT_CONTACT',              'CONTACT_ID',             'UPDATED_TS'),
 ('APPTRACK_DBO_BT_ADDRESS',              'STG.BT_ADDRESS',              'ADDRESS_ID',             'UPDATED_TS'),
 ('APPTRACK_DBO_BT_MERCHANT',             'STG.BT_MERCHANT',             'MERCHANT_ID',            'UPDATED_TS'),
 ('APPTRACK_DBO_BT_BRAND',                'STG.BT_BRAND',                'BRAND_ID',               'UPDATED_TS'),
 ('APPTRACK_DBO_BT_CD_APPLICATION_TYPE',  'STG.BT_CD_APPLICATION_TYPE',  'APPLICATION_TYPE_ID',    'UPDATED_TS'),
 ('APPTRACK_DBO_BT_APPLICATION_STANDING', 'STG.BT_APPLICATION_STANDING', 'APPLICATION_STANDING_ID','UPDATED_TS'),
 ('APPTRACK_DBO_BT_CD_APPLICATION_STATUS','STG.BT_CD_APPLICATION_STATUS','APPLICATION_STATUS_ID',  'UPDATED_TS'),
 ('APPTRACK_DBO_BT_CREDIT_APPRAISAL',     'STG.BT_CREDIT_APPRAISAL',     'CREDIT_APPRAISAL_ID',    'UPDATED_TS'),
 ('HOMER_DBO_BT_NOTIFICATION',            'STG.BT_NOTIFICATION',         'NOTIFICATION_ID',        'UPDATED_TS'),
 ('HOMER_DBO_BT_NOTIFICATION_MAIL',       'STG.BT_NOTIFICATION_MAIL',    'NOTIFICATION_MAIL_ID',   'UPDATED_TS');
-- >>> REPLACE key_cols and order_col with your REAL keys and timestamp columns <<<


/* -----------------------------------------------------------------------------
   STEP 2 — The refresh procedure.
   For each active table, rebuild the staging table from source, deduped:
     QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY ts DESC) = 1
       -> keeps exactly one row per key = the latest by timestamp.
   CREATE OR REPLACE = clean full reload every run (your chosen strategy).
   ----------------------------------------------------------------------------- */
CREATE OR REPLACE PROCEDURE META.REFRESH_STAGING()
RETURNS STRING LANGUAGE SQL AS
$$
DECLARE
  c CURSOR FOR
    SELECT src_table, stg_table, key_cols, order_col
    FROM META.DEDUP_CONFIG WHERE is_active;
  n INT DEFAULT 0;
  errmsg STRING DEFAULT '';
BEGIN
  FOR r IN c DO
    BEGIN
      EXECUTE IMMEDIATE
        'CREATE OR REPLACE TABLE ' || r.stg_table || ' AS ' ||
        'SELECT * FROM ' || r.src_table ||
        ' QUALIFY ROW_NUMBER() OVER (' ||
        '   PARTITION BY ' || r.key_cols ||
        '   ORDER BY ' || r.order_col || ' DESC) = 1';
      n := n + 1;
    EXCEPTION
      WHEN OTHER THEN
        errmsg := errmsg || r.stg_table || ': ' || SQLERRM || ' | ';
    END;
  END FOR;

  IF (errmsg = '') THEN
    RETURN 'SUCCESS: refreshed ' || n || ' staging tables.';
  ELSE
    RETURN 'PARTIAL: refreshed ' || n || '. Errors -> ' || errmsg;
  END IF;
END;
$$;


/* -----------------------------------------------------------------------------
   STEP 3 — Schedule it: a daily TASK that calls the procedure.
   CRON below = 5:00 AM America/Chicago every day. Adjust as needed.
   ----------------------------------------------------------------------------- */
CREATE OR REPLACE TASK META.TSK_REFRESH_STAGING
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = 'USING CRON 0 5 * * * America/Chicago'
AS
  CALL META.REFRESH_STAGING();

-- Tasks are created SUSPENDED; resume to activate the daily schedule:
ALTER TASK META.TSK_REFRESH_STAGING RESUME;


/* -----------------------------------------------------------------------------
   STEP 4 — Test now & verify (don't wait for 5 AM)
   ----------------------------------------------------------------------------- */
-- Run the whole pipeline immediately:
EXECUTE TASK META.TSK_REFRESH_STAGING;

-- Or run the proc directly and read the status message:
CALL META.REFRESH_STAGING();

-- Verify NO duplicates remain (expect 0 rows). Repeat per table/key:
SELECT APPLICATION_ID, COUNT(*) AS dup
FROM STG.BT_APPLICATION
GROUP BY 1 HAVING COUNT(*) > 1;

-- Compare row counts source vs staging (staging should be <= source):
SELECT
  (SELECT COUNT(*) FROM APPTRACK_DBO_BT_APPLICATION) AS src_rows,
  (SELECT COUNT(*) FROM STG.BT_APPLICATION)          AS stg_rows;

-- Task run history (success/failure, duration):
SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
ORDER BY SCHEDULED_TIME DESC
LIMIT 20;


/* -----------------------------------------------------------------------------
   STEP 5 — Operations (handy commands)
   ----------------------------------------------------------------------------- */
-- Pause the daily pipeline:
-- ALTER TASK META.TSK_REFRESH_STAGING SUSPEND;

-- Change the schedule (e.g. 2 AM):
-- ALTER TASK META.TSK_REFRESH_STAGING SUSPEND;
-- ALTER TASK META.TSK_REFRESH_STAGING SET SCHEDULE = 'USING CRON 0 2 * * * America/Chicago';
-- ALTER TASK META.TSK_REFRESH_STAGING RESUME;

-- Add a new table to the pipeline later — just insert a config row:
-- INSERT INTO META.DEDUP_CONFIG (src_table, stg_table, key_cols, order_col)
-- VALUES ('SCHEMA.NEW_SRC', 'STG.NEW_TBL', 'NEW_ID', 'UPDATED_TS');


/* =============================================================================
   NOTES
   - Why full reload: simplest + self-correcting. If source fixes a bad row,
     staging reflects it next morning. Great for low-millions row tables.
   - Composite keys: put 'COL_A, COL_B' in key_cols — PARTITION BY handles it.
   - No timestamp on a table? Use a load-time/sequence column as order_col,
     or for full-row dedup PARTITION BY all columns.
   - Cost control: the task only runs once a day; warehouse auto-suspends when idle.
   - Scaling up: if the 11M-row NOTIFICATION tables make full reload slow/expensive,
     switch THOSE to an incremental Stream+Task design (ask and I'll provide).
   ============================================================================= */
