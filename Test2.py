

# Replace with your actual job_execution_id
my_job_id = "your_job_execution_id_value"

# Filter DataFrame
filtered_df = df.filter(df.job_execution_id == my_job_id)

# Show results
filtered_df.show(truncate=False)
