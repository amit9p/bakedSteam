
set start_dt = (select max(bus.dt) from coaf_db.collab_lab_svcg.resolved_digtal_segment_August23);
set start_dt = dateadd(day, -60, $start_dt);
import os
from datetime import datetime

# Get the current timestamp in the format yyyymmddhhmmss
current_time = datetime.now().strftime("%Y%m%d%H%M%S")

# Define the directory name using the timestamp
folder_name = current_time

# Create the directory in the current working directory
directory_path = os.path.join(os.getcwd(), folder_name)

# Create the directory
os.makedirs(directory_path, exist_ok=True)

print(f"Folder '{folder_name}' created at {directory_path}")


from pyspark.sql.functions import col

# Update data types as per the schema in the second image
df_final = df_final.select(
    col("df_a.business_date").cast("date").alias("business_date"),  # DATE
    col("df_a.run_identifier").cast("int").alias("run_identifier"),  # INTEGER
    col("df_a.output_file_type").cast("string").alias("output_filetype"),  # STRING
    col("df_a.output_record_sequence").cast("int").alias("output_record_sequence"),  # INTEGER
    col("df_a.output_field_sequence").cast("int").alias("output_field_sequence"),  # INTEGER
    col("df_a.attribute").cast("string").alias("attribute"),  # STRING
    col("final_formatted").cast("string").alias("formatted"),  # STRING
    col("df_a.tokenization").cast("string").alias("tokenization"),  # NULLABLE STRING
    col("df_a.account_number").cast("string").alias("account_number"),  # NULLABLE STRING
    col("df_a.segment").cast("string").alias("segment")  # STRING
)

# Print schema and show some data for verification
df_final.printSchema()
df_final.show(n=500, truncate=False)
