
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Filter DataFrame Example").getOrCreate()

# Sample data for demonstration
data = [
    (1, 20230701, "type1", 123, 1, "attr1", "format1", "token1", "acc1", "segment1"),
    (2, 20230702, "type2", 124, 2, "attr2", "format2", "token2", "acc2", "segment2"),
    (3, 20230703, "type3", 125, 3, "attr3", "format3", "token3", "acc3", "segment3"),
    (4, 20230704, "type4", 126, 4, "attr4", "format4", "token4", "acc4", "segment4"),
    (5, 20230705, "type5", 127, 5, "attr5", "format5", "token5", "acc5", "segment5"),
    (6, 20230706, "type6", 128, 6, "attr6", "format6", "token6", "acc6", "segment6"),
    (7, 20230707, "type7", 129, 7, "attr7", "format7", "token7", "acc7", "segment7"),
    (8, 20230708, "type8", 130, 8, "attr8", "format8", "token8", "acc8", "segment8"),
    (9, 20230709, "type9", 131, 9, "attr9", "format9", "token9", "acc9", "segment9"),
    (10, 20230710, "type10", 132, 10, "attr10", "format10", "token10", "acc10", "segment10"),
    (11, 20230711, "type11", 133, 11, "attr11", "format11", "token11", "acc11", "segment11"),
    (12, 20230712, "type12", 134, 12, "attr12", "format12", "token12", "acc12", "segment12")
]

columns = ["run_identifier", "business_date", "output_file_type", "output_record_sequence", "output_field_sequence",
           "attribute", "formatted", "tokenization", "account_number", "segment"]

df = spark.createDataFrame(data, columns)

# Get the top 10 output_record_sequence values
top_sequences = df.select("output_record_sequence").distinct().orderBy("output_record_sequence").limit(10)

# Collect the values into a list
top_sequences_list = [row.output_record_sequence for row in top_sequences.collect()]

# Filter the main DataFrame based on the top 10 output_record_sequence values
filtered_df = df.filter(df.output_record_sequence.isin(top_sequences_list))

# Show the result DataFrame
filtered_df.show(truncate=False)
