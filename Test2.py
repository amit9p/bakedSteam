

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
