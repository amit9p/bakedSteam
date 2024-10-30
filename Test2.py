
2. Partition Processing:

concatenate_partition processes each partition to concatenate rows within it, reducing memory usage.

mapPartitions applies this function to each partition of the RDD, building strings partition by partition.



3. Final Collection and Join:

Collects the strings from each partition and joins them into a single string.




№###########
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import StringType, IntegerType
import logging

# Configure logger
logger = logging.getLogger(__name__)

# Function to format the DataFrame
def format(df: DataFrame, record_separator: str, field_separator: str) -> str:
    # Convert required columns to integers for sorting
    numeric_df = df.withColumn("OUTPUT_RECORD_SEQUENCE", col("OUTPUT_RECORD_SEQUENCE").cast(IntegerType())) \
                   .withColumn("OUTPUT_FIELD_SEQUENCE", col("OUTPUT_FIELD_SEQUENCE").cast(IntegerType()))

    # Order and select necessary columns
    ordered_df = numeric_df.select("formatted", "OUTPUT_RECORD_SEQUENCE", "OUTPUT_FIELD_SEQUENCE") \
                           .orderBy(col("OUTPUT_RECORD_SEQUENCE").asc(), col("OUTPUT_FIELD_SEQUENCE").asc())

    # Pivot DataFrame
    pivot_df = ordered_df.groupBy("OUTPUT_RECORD_SEQUENCE") \
                         .pivot("OUTPUT_FIELD_SEQUENCE") \
                         .agg(F.first("formatted")) \
                         .na.fill("")

    # Convert all fields to strings
    pivot_df = pivot_df.select([col(c).cast(StringType()).alias(c) for c in pivot_df.columns])

    # Concatenate all columns into a single string with the specified separator
    rows = pivot_df.select(concat_ws(field_separator, *pivot_df.columns).alias("metro2_string"))

    # Define the function to process each partition
    def concatenate_partition(partition):
        partition_string = ""
        for row in partition:
            # Append each row's string to the partition string
            partition_string += row.metro2_string + record_separator
        # Return a single-element iterator with the concatenated string
        yield partition_string

    # Apply mapPartitions on the DataFrame's RDD and collect the results
    final_output_rdd = rows.rdd.mapPartitions(concatenate_partition)

    # Collect partitioned strings and join them into the final output string
    final_output_string = record_separator.join(final_output_rdd.collect())

    # Log the length of the final output string for verification
    logger.debug(f"Final output string has {len(final_output_string)} characters")

    return final_output_string
