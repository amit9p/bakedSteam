

CREATE OR REPLACE TASK my_daily_task
  WAREHOUSE = my_warehouse
  SCHEDULE = 'USING CRON 0 0 * * * UTC'  -- Executes daily at midnight UTC
AS
  INSERT INTO my_table_archive SELECT * FROM my_table WHERE event_date = CURRENT_DATE();


CREATE MATERIALIZED VIEW my_materialized_view
AS
SELECT column1, SUM(column2) FROM my_table GROUP BY column1;
