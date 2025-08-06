
df_filtered.coalesce(1).write.mode("overwrite").option("header", True).csv("output_path")
