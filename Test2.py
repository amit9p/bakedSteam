
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit
import logging

logger = logging.getLogger(__name__)

def process_file_type(df: DataFrame, record_separator: str) -> str:
    try:
        # Selecting required columns and ordering based on row_position and column_position
        ordered_df = df.select("value", "row_position", "column_position").orderBy(
            col("row_position").asc(), col("column_position").asc()
        )

        # Fill None with empty strings
        ordered_df = ordered_df.fillna({"value": ""})

        # Pivot the DataFrame
        pivot_df = ordered_df.groupBy("row_position").pivot("column_position").agg(lit("")).na.fill("")

        # Concatenate all columns into a single string for each row, resulting in a Metro2 string
        columns = [col(column) for column in pivot_df.columns if column != "row_position"]
        pivot_df = pivot_df.withColumn("metro2_string", concat_ws("", *columns))

        # Collect the Metro2 strings and join them with the record separator
        metro2_strings = pivot_df.select("metro2_string").rdd.flatMap(lambda x: x).collect()
        final_output_string = record_separator.join(metro2_strings)

        return final_output_string

    except Exception as e:
        logger.error(e)
        return ""

if __name__ == "__main__":
    spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()
    # Example DataFrame
    data = [
        ("value1", 1, 1),
        ("value2", 1, 2),
        ("value3", 2, 1),
        ("value4", 2, 2)
    ]
    columns = ["value", "row_position", "column_position"]
    df = spark.createDataFrame(data, columns)

    record_separator = "|"
    result = process_file_type(df, record_separator)
    print(result)
