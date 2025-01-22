
SELECT 
    *,
    RANK() OVER (
        PARTITION BY PMT_PLAN_DETL_ID, 
                     SLTN_ENRLNT_ID, 
                     EVT_TYPE_TXT, 
                     PMT_STAT_TXT, 
                     PMT_UPDTD_TS, 
                     PMT_LAST_STAT_TS, 
                     LEGL_AFIL_CD, 
                     PMT_CAT_TXT, 
                     PMT_CRETD_BY_ID, 
                     BUS_DT
        ORDER BY COAF_PUBLN_ID ASC
    ) AS RN
FROM (
    SELECT 
        COAF_DB.COAF_TB.DELQ_ACCT_SLTN_PMT_PLAN_FACT_SEQ.NEXTVAL AS DELQ_ACCT_SLTN_PMT_PLAN_FACT_ID,
        ACCT_DIM_ID,
        LOAN_ACCT_NUM,
        SLTN_ENRLNT_ID,
        PMT_PLAN_DETL_ID,
        SRC_PRIM_KEY,
        SOR_ID,
        LEGL_AFIL_CD,
        COAF_PUBLN_ID,
        BUS_DT,
        BUS_TS,
        ELT_AUDT_TAG,
        LOAN_ACCT_ID,
        PMT_ID,
        OLD_PMT_ID,
        EVT_TYPE_TXT,
        PMT_AMT,
        PMT_RECVD_AMT,
        PMT_CAT_TXT,
        PMT_STAT_TXT,
        PMT_REVERSE_REASN_TXT,
        PMT_EFFT_DT,
        PMT_EDITD_TS,
        PMT_UPDTD_TS,
        PMT_CRETD_TS,
        PMT_LAST_STAT_TS
    FROM COAF_DB.COAF_TB.DELQ_ACCT_SLTN_PMT_PLAN_FACT
) AS SOURCE_TABLE;
