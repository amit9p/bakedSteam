
Root Cause

1. Type Inconsistency:

The code is casting OUTPUT_RECORD_SEQUENCE to LongType and OUTPUT_FIELD_SEQUENCE to IntegerType. However, a downstream operation (e.g., pivot) expects consistent types for these fields.

If one column is LongType and the other is IntegerType, Spark throws this error because it cannot reconcile the types.



2. Pivot Operation:

In operations like pivot, Spark uses hash keys that require the column types to be consistent.



3. Small Dataset Testing:

The issue may not appear with larger datasets if implicit type promotion occurs, but with smaller datasets, Spark enforces strict type validation.



from pyspark.sql.functions import col
from pyspark.sql.types import LongType

def format(df: DataFrame, record_separator: str, field_separator: str) -> DataFrame:
    # Convert both columns to the same type (e.g., LongType)
    numeric_df = (
        df.withColumn(OUTPUT_RECORD_SEQUENCE, col(OUTPUT_RECORD_SEQUENCE).cast(LongType()))
          .withColumn(OUTPUT_FIELD_SEQUENCE, col(OUTPUT_FIELD_SEQUENCE).cast(LongType()))  # Changed to LongType
    )
    return numeric_df
