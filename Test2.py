
DELETE FROM table_name
WHERE id_column NOT IN (
    SELECT MIN(id_column)
    FROM table_name
    GROUP BY column1, column2, column3
);
