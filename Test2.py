

export PYTHONPATH=$(pwd)
python -m ecbr_calculations.edq.scripts.edq_rule_engine --env nonprod --rule_type non_suppressed --field_name first_name portfolio_type


python -m ecbr_calculations.edq.scripts.edq_rule_engine --env nonprod --rule_type non_suppressed --field_name first_name portfolio_type



-- Step 1: Identify missing records based on all data columns except audit columns (B, C)
WITH missing_records AS (
    SELECT * FROM Table_B
    EXCEPT
    SELECT * FROM Table_A
)

-- Step 2: Insert missing records including audit columns (B, C)
INSERT INTO Table_A
SELECT * FROM missing_records;




#####


-- Step 1: Find missing records
WITH missing_records AS (
    SELECT B.* 
    FROM Table_B B
    LEFT JOIN Table_A A
    ON B.A = A.A 
    AND B.D = A.D 
    AND B.Col1 = A.Col1 
    AND B.Col2 = A.Col2 
    AND B.Col3 = A.Col3 -- (Include all data columns except B and C)
    WHERE A.A IS NULL  -- If no match, it means the record is missing
)

-- Step 2: Insert missing records
INSERT INTO Table_A
SELECT * FROM missing_records;


#####

-- Step 1: Identify missing records based on columns A and D
WITH missing_records AS (
    SELECT A, D
    FROM Table_B
    WHERE (A, D) NOT IN (SELECT A, D FROM Table_A)
)

-- Step 2: Insert missing records along with B and C (audit columns) into Table_A
INSERT INTO Table_A (A, B, C, D)
SELECT B.A, B.B, B.C, B.D
FROM Table_B B
JOIN missing_records M ON B.A = M.A AND B.D = M.D;



# Constants for Bankruptcy Status
BANKRUPTCY_STATUS_OPEN = "open"
BANKRUPTCY_STATUS_DISCHARGED = "discharged"
BANKRUPTCY_CHAPTER_13 = "13"
