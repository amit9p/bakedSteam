-- Insert today's data into the target table from the source table, avoiding duplicates
INSERT INTO target_table
SELECT *
FROM source_table
WHERE date_column = CURRENT_DATE
AND NOT EXISTS (
    SELECT 1 
    FROM target_table t
    WHERE t.unique_id_column = source_table.unique_id_column
);
