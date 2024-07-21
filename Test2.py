

df1.createOrReplaceTempView("t1")
query = """
SELECT *,
       LENGTH(formatted) AS formatted_length
FROM t1
WHERE output_record_sequence IN (2, 3, 4)
ORDER BY output_record_sequence, output_field_sequence ASC
"""

# Execute the query
df1 = spark.sql(query)

# Show the results
df1.show(n=500, truncate=False)
df1.printSchema()
