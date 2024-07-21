

# List of columns in the desired order (same as df2)
columns_in_order = [
    "business_date", "run_identifier", "output_file_type", "output_record_sequence", 
    "output_field_sequence", "attribute", "formatted", "tokenization", 
    "account_number", "segment"
]

# Reorder new_df to match the column order of df2
new_df_reordered = new_df.select(columns_in_order)

# Perform the union
df3 = df2.union(new_df_reordered)

# Show the new DataFrame
df3.show()

#####


from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialize Spark session
spark = SparkSession.builder.appName("RenameColumns").getOrCreate()

# Assuming you already have your DataFrame loaded, e.g. df
# Sample data to create a DataFrame (replace this with your actual DataFrame loading code)
data = [(1, "acc1", "seg1", "attr1", "val1", 1, 1, "type1", "2023-07-21"),
        (2, "acc2", "seg2", "attr2", "val2", 2, 2, "type2", "2023-07-22")]

columns = ["run_id", "account_id", "segment", "attribute", "value", "row_position", "column_position", "file_type", "business_date"]

df = spark.createDataFrame(data, columns)

# Rename columns and add new column with null values
new_df = df.withColumnRenamed("account_id", "acc_id") \
           .withColumnRenamed("row_position", "row_pos") \
           .withColumnRenamed("column_position", "col_pos") \
           .withColumn("tokenization", lit(None))

# Show the new DataFrame
new_df.show()
