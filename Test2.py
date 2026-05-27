/* =============================================================================
   DEPLOY SNOWPARK PIPELINE INSIDE SNOWFLAKE  (Stored Proc + Daily Task)
   -----------------------------------------------------------------------------
   This wraps the Snowpark Python dedup logic in a Python stored procedure,
   then schedules it daily with a Task. Everything runs inside Snowflake —
   no laptop, no external scheduler needed.
   ============================================================================= */

USE ROLE      SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE  PHDP_CARD_PRTNR_NPI_BT;
CREATE SCHEMA IF NOT EXISTS STG;
CREATE SCHEMA IF NOT EXISTS META;


/* -----------------------------------------------------------------------------
   STEP 1 — Snowpark Python stored procedure (the whole pipeline)
   ----------------------------------------------------------------------------- */
CREATE OR REPLACE PROCEDURE META.SP_REFRESH_STAGING()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'run'
AS
$$
from snowflake.snowpark import Session, Window
from snowflake.snowpark.functions import row_number, col

DATABASE   = "PHDP_CARD_PRTNR_NPI_BT"
STG_SCHEMA = "STG"

# (source_table, staging_table, [key_cols], order_col)  -- edit to your real columns
TABLES = [
    ("APPTRACK_DBO_BT_APPLICATION",           "BT_APPLICATION",          ["APPLICATION_ID"],          "UPDATED_TS"),
    ("APPTRACK_DBO_BT_APPLICATION_CONTACT",   "BT_APPLICATION_CONTACT",  ["APPLICATION_CONTACT_ID"],  "UPDATED_TS"),
    ("APPTRACK_DBO_BT_CD_CONTACT_TYPE",       "BT_CD_CONTACT_TYPE",      ["CONTACT_TYPE_ID"],         "UPDATED_TS"),
    ("APPTRACK_DBO_BT_CONTACT",               "BT_CONTACT",              ["CONTACT_ID"],             "UPDATED_TS"),
    ("APPTRACK_DBO_BT_ADDRESS",               "BT_ADDRESS",              ["ADDRESS_ID"],             "UPDATED_TS"),
    ("APPTRACK_DBO_BT_MERCHANT",              "BT_MERCHANT",             ["MERCHANT_ID"],            "UPDATED_TS"),
    ("APPTRACK_DBO_BT_BRAND",                 "BT_BRAND",                ["BRAND_ID"],               "UPDATED_TS"),
    ("APPTRACK_DBO_BT_CD_APPLICATION_TYPE",   "BT_CD_APPLICATION_TYPE",  ["APPLICATION_TYPE_ID"],     "UPDATED_TS"),
    ("APPTRACK_DBO_BT_APPLICATION_STANDING",  "BT_APPLICATION_STANDING", ["APPLICATION_STANDING_ID"], "UPDATED_TS"),
    ("APPTRACK_DBO_BT_CD_APPLICATION_STATUS", "BT_CD_APPLICATION_STATUS",["APPLICATION_STATUS_ID"],   "UPDATED_TS"),
    ("APPTRACK_DBO_BT_CREDIT_APPRAISAL",      "BT_CREDIT_APPRAISAL",     ["CREDIT_APPRAISAL_ID"],     "UPDATED_TS"),
    ("HOMER_DBO_BT_NOTIFICATION",             "BT_NOTIFICATION",         ["NOTIFICATION_ID"],        "UPDATED_TS"),
    ("HOMER_DBO_BT_NOTIFICATION_MAIL",        "BT_NOTIFICATION_MAIL",    ["NOTIFICATION_MAIL_ID"],    "UPDATED_TS"),
]

def run(session: Session) -> str:
    done, errors = [], []
    for src, stg, keys, order_col in TABLES:
        try:
            w = Window.partition_by([col(k) for k in keys]).order_by(col(order_col).desc())
            df = (session.table(f"{DATABASE}.{src}")
                         .with_column("RN", row_number().over(w))
                         .filter(col("RN") == 1)
                         .drop("RN"))
            df.write.save_as_table(f"{DATABASE}.{STG_SCHEMA}.{stg}", mode="overwrite")
            done.append(stg)
        except Exception as e:
            errors.append(f"{stg}: {e}")
    msg = f"Refreshed {len(done)} tables."
    if errors:
        msg += " ERRORS -> " + " | ".join(errors)
    return msg
$$;


/* -----------------------------------------------------------------------------
   STEP 2 — Test it now
   ----------------------------------------------------------------------------- */
CALL META.SP_REFRESH_STAGING();


/* -----------------------------------------------------------------------------
   STEP 3 — Schedule daily with a Task
   ----------------------------------------------------------------------------- */
CREATE OR REPLACE TASK META.TSK_SNOWPARK_REFRESH
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = 'USING CRON 0 5 * * * America/Chicago'   -- 5 AM daily
AS
  CALL META.SP_REFRESH_STAGING();

ALTER TASK META.TSK_SNOWPARK_REFRESH RESUME;


/* -----------------------------------------------------------------------------
   STEP 4 — Verify
   ----------------------------------------------------------------------------- */
-- no duplicates? expect 0 rows:
SELECT APPLICATION_ID, COUNT(*) FROM STG.BT_APPLICATION GROUP BY 1 HAVING COUNT(*) > 1;

-- task history:
SELECT NAME, STATE, SCHEDULED_TIME, ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
ORDER BY SCHEDULED_TIME DESC LIMIT 10;

-- pause if needed:
-- ALTER TASK META.TSK_SNOWPARK_REFRESH SUSPEND;
