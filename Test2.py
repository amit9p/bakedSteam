

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, LongType

# --- 0) Your 10 fixed values (from your screenshot) ---
customer_ids = [
    1001315206, 1004043965, 1004043969, 1004043979, 1004043992,
    1004043993, 1004043996, 1004044027, 1004044038, 1004044038
]
account_ids = [
    7777771001, 7777771002, 7777771003, 7777771004, 7777771005,
    7777771006, 7777771007, 7777771008, 7777771009, 7777771010
]

# --- 1) Start from your DF; drop sdp4_metadata if present ---
df1 = df.drop("sdp4_metadata") if "sdp4_metadata" in df.columns else df

# --- 2) Add row numbers and take first 10 rows deterministically ---
w = Window.orderBy(F.monotonically_increasing_id())
df10 = df1.withColumn("row_num", F.row_number().over(w)).orderBy("row_num").limit(10)

# --- 3) Build mapping DF with an explicit schema (prevents createDataFrame errors) ---
mapping_data = [(customer_ids[i], account_ids[i], i+1) for i in range(10)]
mapping_schema = StructType([
    StructField("customer_id", LongType(), False),
    StructField("account_id",  LongType(), False),
    StructField("row_num",     LongType(), False),
])
mapping_df = spark.createDataFrame(mapping_data, schema=mapping_schema)

# --- 4) Join by row_num to replace ids with your exact values ---
df_joined = (
    df10.drop("customer_id", "account_id")  # remove existing cols if present
        .join(mapping_df, on="row_num", how="inner")
        .drop("row_num")
)

# --- 5) Add instnc_id as the LAST column ---
df_with_inst = df_joined.withColumn("instnc_id", F.lit("20251001"))
cols_reordered = [c for c in df_with_inst.columns if c != "instnc_id"] + ["instnc_id"]
df_final = df_with_inst.select(cols_reordered)

# --- 6) Show final 10 records with your exact customer/account ids ---
df_final.show(truncate=False)
