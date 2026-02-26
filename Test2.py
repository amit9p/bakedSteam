SUFFIX_THREE = ["3", "iii"]
SUFFIX_FOUR = ["4", "iv"]
SUFFIX_FIVE = ["5", "v"]
SUFFIX_SIX = ["6", "vi"]
SUFFIX_SEVEN = ["7", "vii"]
SUFFIX_EIGHT = ["8", "viii"]
SUFFIX_NINE = ["9", "ix"]





behave tests/ecbr_calculations/features/consumer_features/base_passthrough.feature \
  --name "Process account_type from parquet file primary"

from pyspark.sql.functions import when, to_date, current_date

result_df = input_df.withColumn(
    EcbrCalculatorOutput.formatted_date_of_account_information.str,
    when(
        account_status_13_or_64,
        to_date(ECBRCardDFSAccountsPrimary.transaction_date)
    ).otherwise(
        current_date()
    )
)


from datetime import date
import datetime
from pyspark.sql import Row

def test_date_of_account_information_status_13_or_64(spark):
    rows = [
        Row(
            account_id=1,
            customer_id=100,
            credit_bureau_account_status=CREDIT_BUREAU_ACCOUNT_STATUS_13,
            transaction_date=datetime.datetime(2024, 6, 1, 10, 30, 0),
        ),
        Row(
            account_id=2,
            customer_id=200,
            credit_bureau_account_status=CREDIT_BUREAU_ACCOUNT_STATUS_64,
            transaction_date=datetime.datetime(2024, 6, 2, 15, 45, 0),
        ),
    ]

    df = spark.createDataFrame(rows, schema=schema)

    result_df = date_of_account_information(df)

    expected_df = create_partially_filled_dataset(
        spark,
        EcbrCalculatorOutput,
        data=[
            {
                EcbrCalculatorOutput.account_id: 1,
                EcbrCalculatorOutput.customer_id: 100,
                EcbrCalculatorOutput.formatted_date_of_account_information: date(2024, 6, 1),
            },
            {
                EcbrCalculatorOutput.account_id: 2,
                EcbrCalculatorOutput.customer_id: 200,
                EcbrCalculatorOutput.formatted_date_of_account_information: date(2024, 6, 2),
            },
        ],
    ).select(
        EcbrCalculatorOutput.account_id,
        EcbrCalculatorOutput.customer_id,
        EcbrCalculatorOutput.formatted_date_of_account_information,
    )

    assert_df_equality(result_df, expected_df, ignore_row_order=True)




from datetime import date
import datetime
from pyspark.sql import Row

def test_date_of_account_information_otherwise_condition(spark):
    rows = [
        Row(
            account_id=3,
            customer_id=300,
            credit_bureau_account_status=99,  # NOT 13 or 64
            transaction_date=datetime.datetime(2024, 6, 10, 12, 0, 0),
        )
    ]

    df = spark.createDataFrame(rows, schema=schema)

    result_df = date_of_account_information(df)

    today_date = date.today()

    expected_df = create_partially_filled_dataset(
        spark,
        EcbrCalculatorOutput,
        data=[
            {
                EcbrCalculatorOutput.account_id: 3,
                EcbrCalculatorOutput.customer_id: 300,
                EcbrCalculatorOutput.formatted_date_of_account_information: today_date,
            }
        ],
    ).select(
        EcbrCalculatorOutput.account_id,
        EcbrCalculatorOutput.customer_id,
        EcbrCalculatorOutput.formatted_date_of_account_information,
    )

    assert_df_equality(result_df, expected_df, ignore_row_order=True)
