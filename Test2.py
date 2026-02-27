
df.coalesce(1) \
  .write \
  .mode("overwrite") \
  .option("header", "true") \
  .csv("/path/to/output_dir")

from pyspark.sql import DataFrame
from pyspark.sql.functions import when, lower, col, lit

def j2_gen_code(df: DataFrame) -> DataFrame:
    gen_col = ECBRCardDFSAccountsSecondary.gen_code.str

    result_df = (
        df.withColumn(
            EcbrCalculatorOutput.j2_gen_code.str,
            when(
                lower(col(gen_col)).isin(constants.SUFFIX_JUNIOR),
                lit(constants.GENERATION_CODE_J.upper())
            )
            .when(
                lower(col(gen_col)).isin(constants.SUFFIX_SENIOR),
                lit(constants.GENERATION_CODE_S.upper())
            )
            .when(
                lower(col(gen_col)) == constants.SUFFIX_THREE,
                lit(constants.GENERATION_CODE_THREE)
            )
            .when(
                lower(col(gen_col)) == constants.SUFFIX_FOUR,
                lit(constants.GENERATION_CODE_FOUR)
            )
            .when(
                lower(col(gen_col)) == constants.SUFFIX_FIVE,
                lit(constants.GENERATION_CODE_FIVE)
            )
            .when(
                lower(col(gen_col)) == constants.SUFFIX_SIX,
                lit(constants.GENERATION_CODE_SIX)
            )
            .when(
                lower(col(gen_col)) == constants.SUFFIX_SEVEN,
                lit(constants.GENERATION_CODE_SEVEN)
            )
            .when(
                lower(col(gen_col)) == constants.SUFFIX_EIGHT,
                lit(constants.GENERATION_CODE_EIGHT)
            )
            .when(
                lower(col(gen_col)) == constants.SUFFIX_NINE,
                lit(constants.GENERATION_CODE_NINE)
            )
            .otherwise(lit(constants.GENERATION_CODE_DEFAULT))
        )
    )

    return result_df.select(
        EcbrCalculatorOutput.account_id,
        EcbrCalculatorOutput.customer_id,
        EcbrCalculatorOutput.j2_gen_code,
    )



SUFFIX_THREE = ["3", "iii"]
SUFFIX_FOUR = ["4", "iv"]
SUFFIX_FIVE = ["5", "v"]
SUFFIX_SIX = ["6", "vi"]
SUFFIX_SEVEN = ["7", "vii"]
SUFFIX_EIGHT = ["8", "viii"]
SUFFIX_NINE = ["9", "ix"]

from pyspark.sql.functions import lower, col, lit, when

def j2_gen_code(df: DataFrame) -> DataFrame:
    result_df = df.withColumn(
        EcbrCalculatorOutput.j2_gen_code.str,
        when(
            lower(col(ECBRCrdDFSAccountsSecondary.gen_code.str))
            .isin(constants.SUFFIX_JUNIOR),
            lit(constants.GENERATION_CODE_J.upper()),
        )
        .when(
            lower(col(ECBRCrdDFSAccountsSecondary.gen_code.str))
            .isin(constants.SUFFIX_SENIOR),
            lit(constants.GENERATION_CODE_S.upper()),
        )
        .when(
            lower(col(ECBRCrdDFSAccountsSecondary.gen_code.str))
            .isin(constants.SUFFIX_THREE),
            lit(constants.GENERATION_CODE_THREE),
        )
        .when(
            lower(col(ECBRCrdDFSAccountsSecondary.gen_code.str))
            .isin(constants.SUFFIX_FOUR),
            lit(constants.GENERATION_CODE_FOUR),
        )
        .when(
            lower(col(ECBRCrdDFSAccountsSecondary.gen_code.str))
            .isin(constants.SUFFIX_FIVE),
            lit(constants.GENERATION_CODE_FIVE),
        )
        .when(
            lower(col(ECBRCrdDFSAccountsSecondary.gen_code.str))
            .isin(constants.SUFFIX_SIX),
            lit(constants.GENERATION_CODE_SIX),
        )
        .when(
            lower(col(ECBRCrdDFSAccountsSecondary.gen_code.str))
            .isin(constants.SUFFIX_SEVEN),
            lit(constants.GENERATION_CODE_SEVEN),
        )
        .when(
            lower(col(ECBRCrdDFSAccountsSecondary.gen_code.str))
            .isin(constants.SUFFIX_EIGHT),
            lit(constants.GENERATION_CODE_EIGHT),
        )
        .when(
            lower(col(ECBRCrdDFSAccountsSecondary.gen_code.str))
            .isin(constants.SUFFIX_NINE),
            lit(constants.GENERATION_CODE_NINE),
        )
        .otherwise(lit(constants.GENERATION_CODE_DEFAULT))
    )

    return result_df.select(
        EcbrCalculatorOutput.account_id,
        EcbrCalculatorOutput.customer_id,
        EcbrCalculatorOutput.j2_gen_code,
    )



------





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
