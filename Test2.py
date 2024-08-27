
-- Insert today's data into the target table from the source table
INSERT INTO target_table
SELECT *
FROM source_table
WHERE date_column = CURRENT_DATE;
