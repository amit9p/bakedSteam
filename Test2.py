
def test_missing_fields_should_not_be_nulltype(self, spark):
    """
    Test that any missing selected field is created with a real datatype,
    so Parquet write will not fail with VOID type.
    """

    data = [
        {
            "account_id": "ACC001",
            "sor_id": "SOR001",
            "sor_customer_id": "CUST001",
        }
    ]

    df = spark.createDataFrame(data)

    result_df = df.select(*FieldSelector.get_calculated_fields(df))

    null_type_cols = [
        field.name
        for field in result_df.schema.fields
        if isinstance(field.dataType, NullType)
    ]

    assert null_type_cols == []


______
from pyspark.sql import functions as F
from pyspark.sql import DataFrame


class FieldSelector:

    @classmethod
    def _select_fields(cls, df: DataFrame, field_specs: list) -> DataFrame:
        selected_cols = []

        for output_col, source_col in field_specs:
            if source_col in df.columns:
                selected_cols.append(
                    F.col(source_col).alias(output_col)
                )
            else:
                selected_cols.append(
                    F.lit(None).cast("string").alias(output_col)
                )

        return df.select(*selected_cols)

Found the root cause of the Glue failure. The column "j2_enterprise_servicing_customer_id" is getting created as "VOID/NullType" because it is missing from the input dataframe and our logic adds it using plain "F.lit(None)". Parquet does not support writing "VOID" columns.

Easy fix: cast missing/null columns explicitly, for example "F.lit(None).cast("string")", especially for ID fields. I will update the common "FieldSelector" logic so missing fields are added with proper datatype instead of "NullType".
