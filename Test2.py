
WITH cte1 AS (
  SELECT 'Apple' AS fruit
),
cte2 AS (
  SELECT 'Red' AS color
)

SELECT *
FROM cte1
CROSS JOIN cte2;

fruit | color
--------------
Apple | Red


bankruptcy_file_date_is_null_or_blank = (
    CustomerInformation.bankruptcy_first_filed_date.isNull() |
    (trim(CustomerInformation.bankruptcy_first_filed_date) == "")
)

start_date_of_delinquency_is_null_or_blank = (
    CCAccount.date_of_first_delinquency.isNull() |
    (trim(CCAccount.date_of_first_delinquency) == "")
)

account_open_date_is_null_or_blank = (
    CCAccount.account_open_date.isNull() |
    (trim(CCAccount.account_open_date) == "")
)


.when(
    bankruptcy_file_date_is_null_or_blank &
    start_date_of_delinquency_is_null_or_blank &
    account_open_date_is_null_or_blank,
    lit(DEFAULT_ERROR_DATE)
)
