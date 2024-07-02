
CREATE MATERIALIZED VIEW my_materialized_view
AS
SELECT column1, SUM(column2) FROM my_table GROUP BY column1;
