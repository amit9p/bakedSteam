expected_df = create_partially_filled_dataset(
    spark,
    EcbrCalculatorOutput,
    data={
        EcbrCalculatorOutput.account_id: [1, 2],
        EcbrCalculatorOutput.customer_id: [11, 22],
        EcbrCalculatorOutput.closed_date: [
            "01012023",
            "02012023",
        ],
    },
).select(
    EcbrCalculatorOutput.account_id,
    EcbrCalculatorOutput.customer_id,
    EcbrCalculatorOutput.closed_date,
)



schema = StructType([
    StructField("account_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("credit_bureau_account_status", IntegerType(), True),
    StructField("transaction_date", TimestampType(), True),
])

df = spark.createDataFrame(rows, schema=schema)



def test_date_of_account_information_otherwise_condition(spark):
    rows = [
        Row(
            account_id=3,
            customer_id=300,
            credit_bureau_account_status=99,  # NOT 13 or 64
            transaction_date=datetime.datetime(
                year=2024, month=6, day=10, hour=12, minute=0, second=0
            ),
        )
    ]

    df = spark.createDataFrame(rows)

    result_df = date_of_account_information(df)

    today_str = datetime.date.today().strftime("%d%m%Y")

    expected_df = create_partially_filled_dataset(
        spark,
        EcbrCalculatorOutput,
        data=[
            {
                EcbrCalculatorOutput.account_id: 3,
                EcbrCalculatorOutput.customer_id: 300,
                EcbrCalculatorOutput.formatted_date_of_account_information: today_str,
            }
        ],
    ).select(
        EcbrCalculatorOutput.account_id,
        EcbrCalculatorOutput.customer_id,
        EcbrCalculatorOutput.formatted_date_of_account_information,
    )

    assert_df_equality(result_df, expected_df, ignore_row_order=True)



______



from pyspark.sql import DataFrame
from pyspark.sql.functions import when, date_format, to_date, current_date

def date_of_account_information(input_df: DataFrame) -> DataFrame:

    account_status_13_or_64 = (
        ECBRCardDFSAccountsPrimary.credit_bureau_account_status
        .isin(CREDIT_BUREAU_ACCOUNT_STATUS_13, CREDIT_BUREAU_ACCOUNT_STATUS_64)
    )

    result_df = input_df.withColumn(
        EcbrCalculatorOutput.formatted_date_of_account_information.str,
        when(
            account_status_13_or_64,
            # transaction_date can be timestamp → format directly
            date_format(
                ECBRCardDFSAccountsPrimary.transaction_date,
                "ddMMyyyy"
            )
        ).otherwise(
            # fallback date, still Spark-native
            date_format(current_date(), "ddMMyyyy")
        )
    )

    return result_df.select(
        EcbrCalculatorOutput.account_id,
        EcbrCalculatorOutput.customer_id,
        EcbrCalculatorOutput.formatted_date_of_account_information
    )



rows = [
    Row(
        account_id=1,
        customer_id=100,
        credit_bureau_account_status=CREDIT_BUREAU_ACCOUNT_STATUS_13,
        transaction_date=datetime.datetime(2024, 6, 1, 10, 30, 0)
    ),
    Row(
        account_id=2,
        customer_id=200,
        credit_bureau_account_status=CREDIT_BUREAU_ACCOUNT_STATUS_64,
        transaction_date=datetime.datetime(2024, 6, 2, 15, 45, 0)
    ),
]
df = spark.createDataFrame(rows)


expected_df = create_partially_filled_dataset(
    spark,
    EcbrCalculatorOutput,
    data=[
        {
            EcbrCalculatorOutput.account_id: 1,
            EcbrCalculatorOutput.customer_id: 100,
            EcbrCalculatorOutput.formatted_date_of_account_information: "01062024",
        },
        {
            EcbrCalculatorOutput.account_id: 2,
            EcbrCalculatorOutput.customer_id: 200,
            EcbrCalculatorOutput.formatted_date_of_account_information: "02062024",
        },
    ],
).select(
    EcbrCalculatorOutput.account_id,
    EcbrCalculatorOutput.customer_id,
    EcbrCalculatorOutput.formatted_date_of_account_information,
)

assert_df_equality(
    result_df,
    expected_df,
    ignore_row_order=True
)
