
CREATE TABLE filtered_diff AS
SELECT t2.audit_col1, t2.audit_col2, t2.audit_col3, t2.audit_col4, t2.audit_col5, 
       t2.data_col1, t2.data_col2, t2.data_col3, t2.data_col4, t2.data_col5
FROM t2
JOIN data_diff d
ON t2.data_col1 = d.data_col1
AND t2.data_col2 = d.data_col2
AND t2.data_col3 = d.data_col3
AND t2.data_col4 = d.data_col4
AND t2.data_col5 = d.data_col5;




WITH data_diff AS (
    SELECT data_col1, data_col2, data_col3, data_col4, data_col5
    FROM t2
    EXCEPT
    SELECT data_col1, data_col2, data_col3, data_col4, data_col5
    FROM t1
)



SELECT t2.audit_col1, t2.audit_col2, t2.audit_col3, t2.audit_col4, t2.audit_col5, 
       t2.data_col1, t2.data_col2, t2.data_col3, t2.data_col4, t2.data_col5
FROM t2
JOIN data_diff d
ON t2.data_col1 = d.data_col1
AND t2.data_col2 = d.data_col2
AND t2.data_col3 = d.data_col3
AND t2.data_col4 = d.data_col4
AND t2.data_col5 = d.data_col5;
