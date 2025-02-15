
WITH t2_with_id AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY data_col1, data_col2, data_col3, data_col4, data_col5 ORDER BY audit_col1) AS row_id
    FROM t2
),
data_diff AS (
    SELECT data_col1, data_col2, data_col3, data_col4, data_col5
    FROM t2_with_id
    EXCEPT
    SELECT data_col1, data_col2, data_col3, data_col4, data_col5
    FROM t1
),
filtered_rows AS (
    SELECT t2.*
    FROM t2_with_id t2
    JOIN data_diff d
    ON t2.data_col1 = d.data_col1
    AND t2.data_col2 = d.data_col2
    AND t2.data_col3 = d.data_col3
    AND t2.data_col4 = d.data_col4
    AND t2.data_col5 = d.data_col5
)
SELECT * FROM filtered_rows;
