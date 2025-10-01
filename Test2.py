


from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, LongType

# --- Step 1: Create mapping DataFrame with your 10 values ---
mapping_data = [
    (1001315206, 7777771001),
    (1004043965, 7777771002),
    (1004043969, 7777771003),
    (1004043979, 7777771004),
    (1004043992, 7777771005),
    (1004043993, 7777771006),
    (1004043996, 7777771007),
    (1004044027, 7777771008),
    (1004044038, 7777771009),
    (1004044038, 7777771010)
]

schema = StructType([
    StructField("customer_id", LongType(), False),
    StructField("account_id", LongType(), False)
])

mapping_df = spark.createDataFrame(mapping_data, schema=schema)

# --- Step 2: Limit original df to 10 rows (deterministically) ---
w = Window.orderBy(F.monotonically_increasing_id())
df10 = df.withColumn("row_num", F.row_number().over(w)).filter("row_num <= 10")

# --- Step 3: Add row_num to mapping_df ---
mapping_df = mapping_df.withColumn("row_num", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))

# --- Step 4: Replace customer_id & account_id with mapping values ---
df_final = (
    df10.drop("customer_id", "account_id")   # drop old ones
        .join(mapping_df, on="row_num", how="inner")
        .drop("row_num")
)

df_final.show(truncate=False)
