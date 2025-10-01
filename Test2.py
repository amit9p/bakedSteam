


from pyspark.sql import functions as F
from pyspark.sql import Row

# Your 10 fixed values (from screenshot)
customer_ids = [
    1001315206, 1004043965, 1004043969, 1004043979, 1004043992,
    1004043993, 1004043996, 1004044027, 1004044038, 1004044038
]
account_ids = [
    7777771001, 7777771002, 7777771003, 7777771004, 7777771005,
    7777771006, 7777771007, 7777771008, 7777771009, 7777771010
]

# Add row_num for aligning
df1 = df.drop("sdp4_metadata")
df1 = df1.withColumn("row_num", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))

# Create mapping DataFrame with your values
pairs = list(zip(customer_ids, account_ids, range(1, 11)))
mapping_df = spark.createDataFrame(pairs, ["customer_id", "account_id", "row_num"])

# Join to replace
df2 = (
    df1.join(mapping_df, on="row_num", how="inner")
       .drop("row_num")
)

# Add instnc_id at the end
df2 = df2.withColumn("instnc_id", F.lit("20251001"))

# Reorder columns → instnc_id last
cols = [c for c in df2.columns if c != "instnc_id"] + ["instnc_id"]
df_final = df2.select(cols).limit(10)

df_final.show(truncate=False)
