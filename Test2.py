
WITH cte_duplicates AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY column1, column2, column3 -- Columns to identify duplicates
            ORDER BY created_at DESC               -- Choose a column for ordering (e.g., most recent first)
        ) AS rn
    FROM your_table
)
DELETE FROM your_table
WHERE id IN (
    SELECT id
    FROM cte_duplicates
    WHERE rn > 1
);
