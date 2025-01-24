
DELETE FROM your_table
WHERE id NOT IN (
    SELECT MIN(id)
    FROM your_table
    GROUP BY column1, column2, column3 -- Columns to identify duplicates
);





DELETE FROM coaf_db.collab.lab_svcg.DELQ_ACCT_SLTN_PMT_PLAN_FACT
WHERE SRC_PRIM_KEY IN (
    SELECT SRC_PRIM_KEY
    FROM (
        SELECT 
            SRC_PRIM_KEY,
            ROW_NUMBER() OVER (
                PARTITION BY PMT_PLAN_DETL_ID, PMT_EFF_DT -- Columns to identify duplicates
                ORDER BY COAF_PUBLN_ID DESC                -- Choose a column for ordering
            ) AS rn
        FROM coaf_db.collab.lab_svcg.DELQ_ACCT_SLTN_PMT_PLAN_FACT
    ) AS duplicates
    WHERE rn > 1
);
