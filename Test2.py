

from pyspark.sql.functions import col

# Filter for groups with output_record_sequence 2
run_ids_with_2 = df.filter(col("output_record_sequence") == 2).select("run_identifier").distinct()

# Join back to get all records for those groups
df_filtered = df.join(run_ids_with_2, "run_identifier")

# Show the results
df_filtered.show()
