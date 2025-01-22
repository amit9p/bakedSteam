
WITH UniqueRank AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY DELQ_ACCT_SLTN_PMT_PLAN_FACT_ID, SLTN_ENRLNT_ID ORDER BY PMT_UPDTD_TS DESC) AS row_num
    FROM coaf_db_collab.lab_svcg.DELQ_ACCT_SLTN_PMT_PLAN_FACT_rank_fspt_tbl
)
SELECT *
FROM coaf_db_collab.lab_svcg.DELQ_ACCT_SLTN_PMT_PLAN_FACT_delta_fspt_tbl AS delta
INNER JOIN UniqueRank AS rank
ON rank.DELQ_ACCT_SLTN_PMT_PLAN_FACT_ID = delta.DELQ_ACCT_SLTN_PMT_PLAN_FACT_ID
   AND rank.row_num = 1 -- Ensures only one row from the `rank` table
   AND rank.SLTN_ENRLNT_ID = delta.SLTN_ENRLNT_ID
   AND NVL(rank.PMT_STAT_TXT, '*2') = NVL(delta.PMT_STAT_TXT, '*2')
   AND NVL(rank.PMT_UPDTD_TS, '1900-01-01') = NVL(delta.PMT_UPDTD_TS, '1900-01-01')
   AND rank.BUS_TS = delta.BUS_TS
   AND rank.dml_type = 'INSERT';
