
from pyspark.sql import functions as F

# Step 1: Get the latest timestamp
latest_ts = df.select(F.max("created_timestamp_utc_timestamp")).collect()[0][0]

# Step 2: Filter the DataFrame to only rows with the latest timestamp
latest_df = df.filter(F.col("created_timestamp_utc_timestamp") == latest_ts)

# Step 3: Group by field_name and collect unique primary_keys
result_df = (
    latest_df.select("field_name", "primary_key")
    .distinct()
    .groupBy("field_name")
    .agg(F.collect_set("primary_key").alias("unique_primary_keys"))
)

# Show the result
result_df.show(truncate=False)
