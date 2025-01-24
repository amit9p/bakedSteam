
WITH cte_deduplicated AS (
    SELECT 
        delta.DELQ_ACCT_SLTN_PMT_PLAN_FACT_ID,
        delta.ACCT_DIM_ID,
        delta.LOAN_ACCT_NUM,
        delta.SLTN_ENRLMT_ID,
        delta.PMT_PLAN_DETL_ID,
        delta.SRC_PRIM_KEY,
        delta.SOR_ID,
        delta.LEGL_AFIL_CD,
        delta.COAF_PUBLN_ID,
        delta.BUS_DT,
        delta.BUS_TS,
        delta.ELT_AUDT_TAG,
        delta.LOAN_ACCT_ID,
        delta.PMT_ID,
        delta.OLD_PMT_ID,
        delta.EVT_TYPE_TXT,
        delta.PMT_AMT,
        delta.PMT_RECVD_AMT,
        delta.PMT_CAT_TXT,
        delta.PMT_STAT_TXT,
        delta.PMT_REVERSE_REASN_TXT,
        delta.PMT_EFF_DT,
        delta.PMT_EDITD_TS,
        delta.PMT_UPDTD_TS,
        delta.PMT_CRTED_TS,
        delta.PMT_LAST_STAT_TS,
        delta.PMT_CRTED_BY_ID,
        delta.PMT_EDITD_BY_ID,
        delta.ELT_EFF_CLSTR_DT_NUM,
        delta.EFF_END_TS,
        delta.ASCD_PRIM_KEY_SEQ_NUM,
        delta.CURR_REC_IND,
        delta.DLY_CURR_REC_IND,
        delta.CURR_PUBLN_REC_IND,
        ROW_NUMBER() OVER (
            PARTITION BY delta.LOAN_ACCT_NUM, delta.EVT_TYPE_TXT
            ORDER BY delta.BUS_TS DESC
        ) AS rn
    FROM coaf_db.coaf_prod_work.DELQ_ACCT_SLTN_PMT_PLAN_FACT_delta_fspo_tbl AS delta
    INNER JOIN coaf_db.coaf_prod_work.DELQ_ACCT_SLTN_PMT_PLAN_FACT_rank_fspo_tbl AS rank
    ON delta.DELQ_ACCT_SLTN_PMT_PLAN_FACT_ID = rank.DELQ_ACCT_SLTN_PMT_PLAN_FACT_ID
    AND rank.SLTN_ENRLMT_ID = delta.SLTN_ENRLMT_ID
    AND rank.LEGL_AFIL_CD = delta.LEGL_AFIL_CD
    AND NVL(rank.PMT_CAT_TXT, '*2') = NVL(delta.PMT_CAT_TXT, '*2')
    AND NVL(rank.PMT_STAT_TXT, '*2') = NVL(delta.PMT_STAT_TXT, '*2')
    AND NVL(rank.PMT_UPDTD_TS, '1900-01-01') = NVL(delta.PMT_UPDTD_TS, '1900-01-01')
    AND NVL(rank.PMT_LAST_STAT_TS, '1900-01-01') = NVL(delta.PMT_LAST_STAT_TS, '1900-01-01')
    AND NVL(rank.PMT_CRTED_BY_ID, '*2') = NVL(delta.PMT_CRTED_BY_ID, '*2')
    AND rank.BUS_TS = delta.BUS_TS
    AND rank.DML_TYPE = 'INSERT'
    AND rank.PMT_PLAN_DETL_ID = delta.PMT_PLAN_DETL_ID
)
INSERT INTO coaf_db.coaf.tb.DELQ_ACCT_SLTN_PMT_PLAN_FACT (
    DELQ_ACCT_SLTN_PMT_PLAN_FACT_ID,
    ACCT_DIM_ID,
    LOAN_ACCT_NUM,
    SLTN_ENRLMT_ID,
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
    PMT_EFF_DT,
    PMT_EDITD_TS,
    PMT_UPDTD_TS,
    PMT_CRTED_TS,
    PMT_LAST_STAT_TS,
    PMT_CRTED_BY_ID,
    PMT_EDITD_BY_ID,
    ELT_EFF_CLSTR_DT_NUM,
    EFF_END_TS,
    ASCD_PRIM_KEY_SEQ_NUM,
    CURR_REC_IND,
    DLY_CURR_REC_IND,
    CURR_PUBLN_REC_IND
)
SELECT 
    DELQ_ACCT_SLTN_PMT_PLAN_FACT_ID,
    ACCT_DIM_ID,
    LOAN_ACCT_NUM,
    SLTN_ENRLMT_ID,
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
    PMT_EFF_DT,
    PMT_EDITD_TS,
    PMT_UPDTD_TS,
    PMT_CRTED_TS,
    PMT_LAST_STAT_TS,
    PMT_CRTED_BY_ID,
    PMT_EDITD_BY_ID,
    ELT_EFF_CLSTR_DT_NUM,
    EFF_END_TS,
    ASCD_PRIM_KEY_SEQ_NUM,
    CURR_REC_IND,
    DLY_CURR_REC_IND,
    CURR_PUBLN_REC_IND
FROM cte_deduplicated
WHERE rn = 1;
