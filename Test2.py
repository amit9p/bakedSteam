
# Define the SQL query
query = """
SELECT *
FROM data_table
WHERE run_identifier IN (
    SELECT run_identifier
    FROM data_table
    WHERE output_record_sequence = '2'
)
"""

# Execute the query
result_df = spark.sql(query)

# Show the results
result_df.show()
