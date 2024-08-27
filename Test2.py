
-- Step 1: Create a staging table to hold today's data
CREATE OR REPLACE TABLE my_staging_table AS 
SELECT * 
FROM source_table
WHERE date_column = CURRENT_DATE;

-- Step 2: Merge the new data from the staging table into the main table
MERGE INTO my_main_table AS target
USING my_staging_table AS source
ON target.unique_id_column = source.unique_id_column -- Use a unique identifier column to match records
WHEN MATCHED THEN 
    UPDATE SET
    -- List all columns you want to update if a match is found
    target.column1 = source.column1,
    target.column2 = source.column2,
    ...
WHEN NOT MATCHED THEN 
    INSERT (column1, column2, ...)
    VALUES (source.column1, source.column2, ...);

-- Step 3: Optionally, clean up the staging table if it's no longer needed
DROP TABLE IF EXISTS my_staging_table;
