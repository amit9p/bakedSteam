
from pyspark.sql.functions import when

# Rename columns in df2 for clarity
df2 = df2.withColumnRenamed("ssn", "social_security_number") \
         .withColumnRenamed("pan", "consumer_account_number")

# Create a temporary view of df2
df2.createOrReplaceTempView("df2")

# Update df1 based on df2 values and tokenization_type
df1_updated = df1.alias("df1") \
    .join(df2.alias("df2"), 
          (col("df1.attribute") == "Social Security Number") & (col("df1.tokenization_type") == "USTAXID") & (col("df1.value") == col("df2.social_security_number")) |
          (col("df1.attribute") == "Consumer Account Number") & (col("df1.tokenization_type") == "PAN") & (col("df1.value") == col("df2.consumer_account_number")), 
          "left") \
    .withColumn("value", 
                when((col("df1.attribute") == "Social Security Number") & (col("df1.tokenization_type") == "USTAXID"), col("df2.social_security_number"))
                .when((col("df1.attribute") == "Consumer Account Number") & (col("df1.tokenization_type") == "PAN"), col("df2.consumer_account_number"))
                .otherwise(col("df1.value"))) \
    .select(col("df1.run_id"), col("df1.account_id"), col("df1.segment"), col("df1.attribute"), col("df1.value"), 
            col("df1.row_position"), col("df1.column_position"), col("df1.file_type"), col("df1.business_date"), col("df1.tokenization_type"))

# Show the updated DataFrame
df1_updated.show()
