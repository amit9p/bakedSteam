
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ------------------------------------------------------------------
# 1.  Rows you copied from the sheet (image-2)  ➜  build a DataFrame
# ------------------------------------------------------------------
rows = [
    # (equifax,    experian,  transunion,  consumer_account_number)
    ("850B0B1498", "1270246", "1DTV001", "1234567890123456"),
    ("484B0B1456", "1205950", "1DTV003", "1234567890123457"),
    ("19B0B12984", "1109050", "1DTV028", "1234567890123458"),
    # ... add the rest of the rows here ...
]

cols = ["equifax", "experian", "transunion", "consumer_account_number"]
new_ids = spark.createDataFrame(rows, cols)

# ------------------------------------------------------------------
# 2.  Collapse the three agency columns into ONE struct column
# ------------------------------------------------------------------
new_ids_struct = (
    new_ids
    .select(
        F.struct(
            F.col("equifax"),
            F.col("experian"),
            F.col("transunion")
        ).alias("identification_number"),
        "consumer_account_number"          # keep join key
    )
)

# new_ids_struct.printSchema()
# root
#  |-- identification_number: struct (nullable = false)
#  |    |-- equifax: string (nullable = true)
#  |    |-- experian: string (nullable = true)
#  |    |-- transunion: string (nullable = true)
#  |-- consumer_account_number: string (nullable = true)

# ------------------------------------------------------------------
# 3.  (Optional) overwrite the column in your main dataframe
# ------------------------------------------------------------------
# df_raw is the DataFrame from image-1 that already has every other column.
# Join on consumer_account_number and replace the struct.

df_updated = (
    df_raw.alias("left")
    .join(new_ids_struct.alias("right"), on="consumer_account_number", how="left")
    .withColumn(
        "identification_number",
        F.coalesce("right.identification_number", "left.identification_number")
    )
    .drop(F.col("right.identification_number"))  # clean helper col
)

# ------------------------------------------------------------------
# 4.  Verify
# ------------------------------------------------------------------
df_updated.select("identification_number", "consumer_account_number").show(truncate=False)

# ------------------------------------------------------------------
# 5.  (If you just want the struct-only DataFrame)  write as Parquet / CSV
# ------------------------------------------------------------------
#   new_ids_struct.write.mode("overwrite").parquet("/tmp/id_struct")   # column is struct
#   new_ids_struct.write.mode("overwrite").option("header", True).csv("/tmp/id_struct_csv")
