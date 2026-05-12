Hi Thanvi, these fields are not person’s account details by themselves. "account_id" identifies a financial account only when used with "sor_id". "enterprise_servicing_customer_id" identifies a servicing customer identity, which can be a consumer or business depending on the customer type/context. For these SBFE datasets, they are related to small business account/customer records, not directly personal account details.
sql_df = spark.sql("""
    SELECT DISTINCT
        attribute
    FROM df_fs
    WHERE formatted IS NOT NULL
      AND trim(formatted) <> ''
    ORDER BY attribute
""")

sql_df.show(200, False)
