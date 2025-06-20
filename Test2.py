

WITH
cte1 AS (
  SELECT 1 AS id, 'John' AS name
),
cte2 AS (
  SELECT 'john@example.com' AS email, 30 AS age
),
cte3 AS (
  SELECT 'New York' AS location
),
cte4 AS (
  SELECT 50000 AS salary, 'Engineering' AS dept
)

SELECT
  id, name, NULL AS email, NULL AS age, NULL AS location, NULL AS salary, NULL AS dept
FROM cte1

UNION ALL

SELECT
  NULL, NULL, email, age, NULL, NULL, NULL
FROM cte2

UNION ALL

SELECT
  NULL, NULL, NULL, NULL, location, NULL, NULL
FROM cte3

UNION ALL

SELECT
  NULL, NULL, NULL, NULL, NULL, salary, dept
FROM cte4;



____
WITH cte1 AS (
    SELECT 'cte1' AS source, field_a, NULL AS field_b, NULL AS field_c, NULL AS field_d
    FROM table1
),
cte2 AS (
    SELECT 'cte2' AS source, NULL, field_b, NULL, NULL
    FROM table2
),
cte3 AS (
    SELECT 'cte3' AS source, NULL, NULL, field_c, NULL
    FROM table3
),
cte4 AS (
    SELECT 'cte4' AS source, NULL, NULL, NULL, field_d
    FROM table4
)

SELECT * FROM cte1
UNION ALL
SELECT * FROM cte2
UNION ALL
SELECT * FROM cte3
UNION ALL
SELECT * FROM cte4;





when(reactivation_notification_yes, value=lit(DEFAULT_ERROR_DATE).cast("date"))



WITH cte1 AS (
    SELECT id, field_a FROM table1
),
cte2 AS (
    SELECT id, field_b FROM table2
),
cte3 AS (
    SELECT id, field_c FROM table3
),
cte4 AS (
    SELECT id, field_d FROM table4
)

SELECT 
    COALESCE(cte1.id, cte2.id, cte3.id, cte4.id) AS id,
    field_a,
    field_b,
    field_c,
    field_d
FROM cte1
FULL OUTER JOIN cte2 ON cte1.id = cte2.id
FULL OUTER JOIN cte3 ON COALESCE(cte1.id, cte2.id) = cte3.id
FULL OUTER JOIN cte4 ON COALESCE(cte1.id, cte2.id, cte3.id) = cte4.id;




from datetime import datetime
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from typedspark import create_partially_filled_dataset
from ecbr_card_self_service.ecbr_calculations.fields.base.date_closed import date_closed
from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.cc_account import CCAccount
from ecbr_card_self_service.ecbr_calculations.constants import DEFAULT_ERROR_DATE

def test_date_closed(spark: SparkSession):
    # Input test data
    data = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {
                CCAccount.account_id: "1",
                CCAccount.account_close_date: datetime(year=2024, month=12, day=9).date(),
                CCAccount.charge_off_date: datetime(year=2024, month=12, day=10).date(),
            },
            {
                CCAccount.account_id: "2",
                CCAccount.account_close_date: None,
                CCAccount.charge_off_date: datetime(year=2024, month=12, day=8).date(),
            },
            {
                CCAccount.account_id: "3",
                CCAccount.account_close_date: None,  # Blank handled in logic
                CCAccount.charge_off_date: None,
            },
            {
                CCAccount.account_id: "4",
                CCAccount.account_close_date: None,
                CCAccount.charge_off_date: None,
            },
        ]
    )

    result_df = date_closed(data)

    # Expected output
    expected_data = create_partially_filled_dataset(
        spark,
        BaseSegment,
        data=[
            {
                BaseSegment.account_id: "1",
                BaseSegment.date_closed: datetime(year=2024, month=12, day=9).date(),
            },
            {
                BaseSegment.account_id: "2",
                BaseSegment.date_closed: datetime(year=2024, month=12, day=8).date(),
            },
            {
                BaseSegment.account_id: "3",
                BaseSegment.date_closed: DEFAULT_ERROR_DATE,
            },
            {
                BaseSegment.account_id: "4",
                BaseSegment.date_closed: DEFAULT_ERROR_DATE,
            },
        ]
    ).select(BaseSegment.account_id, BaseSegment.date_closed)

    assert_df_equality(result_df, expected_data, ignore_row_order=True)
