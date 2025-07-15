
import pytest
from pyspark.sql import SparkSession

# Helper utilities you already have in your repo:
from tests.common import create_partially_filled_dataset, assert_df_equality

# Schemas and function under test
from ecbr_card_self_service.edq.common.schemas.customer_information import CustomerInformation
from ecbr_card_self_service.edq.common.schemas.sbfe_ad_segment    import ADSegment
from ecbr_card_self_service.edq.local_run.ab.ad_segment          import get_address_line1

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-ad-segment") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_get_address_line1_maps_correctly_and_selects_only_those_columns(spark):
    # 1) Build a full CustomerInformation DF (only populating the two relevant cols)
    cust_df = create_partially_filled_dataset(
        spark,
        CustomerInformation,
        data=[
            {
                CustomerInformation.account_id           : "C1",
                CustomerInformation.customer_address_line_1: "100 Elm St"
            },
            {
                CustomerInformation.account_id           : "C2",
                CustomerInformation.customer_address_line_1: "200 Oak Ave"
            }
        ]
    )

    # 2) (Optionally) trim input to just the two cols—your function itself also drops extras
    input_df = cust_df.select(
        CustomerInformation.account_id,
        CustomerInformation.customer_address_line_1
    )

    # 3) Call the function under test
    result_df = get_address_line1(input_df)

    # 4) Build the expected ADSegment DF
    expected_df = create_partially_filled_dataset(
        spark,
        ADSegment,
        data=[
            { ADSegment.account_id    : "C1",
              ADSegment.address_line_1: "100 Elm St" },
            { ADSegment.account_id    : "C2",
              ADSegment.address_line_1: "200 Oak Ave" }
        ]
    )

    # 5) Assert that we have exactly those two columns and the right data
    assert_df_equality(
        result_df.select(ADSegment.account_id, ADSegment.address_line_1),
        expected_df.select(ADSegment.account_id, ADSegment.address_line_1),
        ignore_row_order=True,
        ignore_nullable=True
    )

______

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from ecbr_card_self_service.edq.common.schemas.customer_information import CustomerInformation
from ecbr_card_self_service.edq.common.schemas.sbfe_ad_segment    import ADSegment

def get_address_line1(cust_information_df: DataFrame) -> DataFrame:
    """
    Maps CustomerInformation.customer_address_line_1 → ADSegment.address_line_1,
    retains only account_id + address_line_1 columns.
    """
    # 1) Overwrite or add the AD address_line_1 column from the customer column
    df_with_ad = cust_information_df.withColumn(
        ADSegment.address_line_1.name,                     # str: the new column name
        CustomerInformation.customer_address_line_1        # Column: the source column
    )
    
    # 2) Select just account_id and the newly-named address_line_1
    return df_with_ad.select(
        ADSegment.account_id,
        ADSegment.address_line_1
    )
____

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from tests.common import create_partially_filled_dataset, assert_df_equality

from ecbr_card_self_service.edq.common.schemas.customer_information import CustomerInformation
from ecbr_card_self_service.edq.common.schemas.sbfe_ad_segment    import ADSegment
from ecbr_card_self_service.edq.local_run.ab.ad_segment          import get_address_line1

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-ad-segment") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_get_address_line1_trims_and_maps(spark):
    # 1) Build a “full” CustomerInformation DF (only need to supply the two relevant cols)
    full_cust_df = create_partially_filled_dataset(
        spark,
        CustomerInformation,
        data=[
            {
              CustomerInformation.account_id           : "acct-1",
              CustomerInformation.customer_address_line_1: "123 Main St",
            },
            {
              CustomerInformation.account_id           : "acct-2",
              CustomerInformation.customer_address_line_1: "456 Oak Ave",
            }
        ]
    )

    # 2) Trim to only the two columns your mapper expects
    trimmed = full_cust_df.select(
        CustomerInformation.account_id,
        CustomerInformation.customer_address_line_1
    )

    # 3) Invoke the function under test
    result_df = get_address_line1(trimmed)

    # 4) Build the expected ADSegment DF
    expected_df = create_partially_filled_dataset(
        spark,
        ADSegment,
        data=[
            {
              ADSegment.account_id    : "acct-1",
              ADSegment.address_line_1: "123 Main St"
            },
            {
              ADSegment.account_id    : "acct-2",
              ADSegment.address_line_1: "456 Oak Ave"
            }
        ]
    )

    # 5) Compare only those two columns, ignoring order & nullability
    assert_df_equality(
        result_df.select(ADSegment.account_id, ADSegment.address_line_1),
        expected_df.select(ADSegment.account_id, ADSegment.address_line_1),
        ignore_row_order=True,
        ignore_nullable=True
    )
