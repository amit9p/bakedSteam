
df = df.select("*").where(col("tokenization") == "PAN").withColumn("length_of_formatted", length(col("formatted")))
df.show(100, False)
