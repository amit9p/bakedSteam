
sql_df = spark.sql("""
    SELECT DISTINCT
        attribute
    FROM df_fs
    WHERE formatted IS NOT NULL
      AND trim(formatted) <> ''
    ORDER BY attribute
""")

sql_df.show(200, False)
