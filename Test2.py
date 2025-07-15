def get_address_line1(cust_df: DataFrame) -> DataFrame:
    """
    Maps CustomerInformation.customer_address_line_1 → ADSegment.address_line_1,
    retains only account_id + address_line_1 columns.
    """
    …


from pyspark.sql.functions import col

def get_address_line1(cust_df):
    return (
        cust_df
        .select(
            col(CustomerInformation.account_id.name).alias(ADSegment.account_id.name),
            col(CustomerInformation.customer_address_line_1.name)
              .alias(ADSegment.address_line_1.name)
        )
    )



import pytest
from pyspark.sql import SparkSession, Row

# adjust these imports to your package layout
from ecbr_card_self_service.edq.common.schemas.customer_information import CustomerInformation
from ecbr_card_self_service.edq.common.schemas.sbfe_ad_segment    import ADSegment
from ecbr_card_self_service.edq.local_run.ab.ad_segment          import get_address_line1

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-get_address_line1") \
        .getOrCreate()

def test_get_address_line1_copies_and_renames(spark):
    # 1) build input DF
    input_rows = [
        Row(account_id="A1", customer_address_line_1="123 Main St"),
        Row(account_id="A2", customer_address_line_1="456 Oak Ave"),
    ]
    cust_df = spark.createDataFrame(input_rows)

    # 2) call your mapper
    out_df = get_address_line1(cust_df)

    # 3) collect and assert
    actual = { (r[ADSegment.account_id.name], r[ADSegment.address_line_1.name])
               for r in out_df.collect() }

    expected = {
        ("A1", "123 Main St"),
        ("A2", "456 Oak Ave"),
    }

    assert actual == expected

    # 4) schema has exactly two fields, in the right order
    names = out_df.schema.names
    assert names == [ADSegment.account_id.name, ADSegment.address_line_1.name]
