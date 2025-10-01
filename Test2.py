from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType
import pyspark.sql.functions as F

# Spark session
spark = SparkSession.builder.getOrCreate()

# Your original DF
df_original = spark.read.parquet("path_to_your_parquet")  # replace with your path

# Mapping values from your image
mapping_data = [
    (4146147380, 1001315206, 7777771001),
    (4146146886, 1004043965, 7777771002),
    (3477618751, 1004043969, 7777771003),
    (3477618760, 1004043979, 7777771004),
    (4454193610, 1004043992, 7777771005),
    (4454193717, 1004043993, 7777771006),
    (3477618797, 1004044027, 7777771007),
    (3477618801, 1004044038, 7777771008),
    (4454193181, 1004044027, 7777771009),
    (3477618821, 1004044038, 7777771010),
]

# Schema for mapping DataFrame
schema = StructType([
    StructField("customer_pk_id", LongType(), False),
    StructField("new_customer_id", LongType(), False),
    StructField("new_account_id", LongType(), False)
])

# Create mapping DataFrame
mapping_df = spark.createDataFrame(mapping_data, schema=schema)

# Join with original DF to replace customer_id and account_id
df_updated = (
    df_original.alias("orig")
    .join(mapping_df.alias("map"), on="customer_pk_id", how="left")
    .withColumn("customer_id", F.col("map.new_customer_id"))
    .withColumn("account_id", F.col("map.new_account_id"))
    .drop("new_customer_id", "new_account_id")
)

# Show updated result (only 10 rows)
df_updated.show(10, truncate=False)


<><><><<
from pyspark.sql import Row

# Your mapping values
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

# Convert into Row objects
rows = [Row(customer_id=int(c), account_id=int(a)) for c, a in mapping_data]

# Create DataFrame (Spark infers schema automatically)
mapping_df = spark.createDataFrame(rows)

mapping_df.show()
mapping_df.printSchema()


-------
mapping_df = spark.createDataFrame(mapping_data, ["customer_id", "account_id"])


from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, LongType

# Mapping values
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

# Define schema
schema = StructType([
    StructField("customer_id", LongType(), False),
    StructField("account_id", LongType(), False)
])

# ✅ Convert into Row objects to avoid type errors
rows = [Row(customer_id=c, account_id=a) for c, a in mapping_data]

# Create DataFrame
mapping_df = spark.createDataFrame(rows, schema=schema)

mapping_df.show()








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
