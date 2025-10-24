
Here’s what that block is doing:

groupBy(OUTPUT_FILE_SEQUENCE, COMPUTE_RECORD_SEQUENCE) → one row per output record.

.pivot(OUTPUT_FIELD_SEQUENCE) → each field position (1,2,3,…) becomes a column.

.agg(first("formatted")) → pick the value for that field.

.fillna("") → blank out any missing fields.

.drop(OUTPUT_FIELD_SEQUENCE) → we no longer need the field-level sequence after the pivot (we still keep file/record ids for ordering).


pivot_df is therefore a wide table: one row per record, columns 1, 2, 3, … holding the already-formatted field strings.

Next step: build the Metro 2 line by concatenating those columns in numeric order (no delimiter), e.g. concat_ws("", col("1"), col("2"), …). After we produce the final metro2_string, we can drop the file/record ids too if the output is just the flat text.



from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

# Example DataFrame
df = spark.createDataFrame([
    (1044765111, 1039593071, '1J', None),
    (1044765186, 1039592951, '1J', None),
    (1044765161, 1039592991, '1J', None)
], ['account_id', 'customer_id', 'ownership_type_ecoa', 'consumer_info_indicator'])

# ✅ Add new column 'customer_pk_id' with same value as account_id
df = df.withColumn("customer_pk_id", F.col("account_id").cast("long"))

# ✅ Reorder columns so 'customer_pk_id' is first
final_df = df.select(
    "customer_pk_id",
    "account_id",
    "customer_id",
    "ownership_type_ecoa",
    "consumer_info_indicator"
)

# Show result
final_df.show(truncate=False)
final_df.printSchema()
