
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("overwrite-identification-number")
    .getOrCreate()
)

# ─────────────────────────────────────────────
# 1 ▸ Load your existing Parquet
# ─────────────────────────────────────────────
PATH_IN  = "/path/to/original/accounts.parquet"
df_raw   = spark.read.parquet(PATH_IN)

# ─────────────────────────────────────────────
# 2 ▸ Build a tiny mapping DataFrame
#     (Equifax, Experian, TransUnion + KEY)
#     ▼  Paste the rows from your sheet ▼
# ─────────────────────────────────────────────
rows = [
    ("850BB0149B", "1270246",  "1DTV001", "1234567890123456"),
    ("484BB01456", "1205950",  "1DTV003", "1234567890123457"),
    ("190BB12984", "1109050",  "1DTV228", "1234567890123458"),
    ("190BB13037", "1110080",  "1DTV229", "1234567890123459"),
    ("190BB13028", "1110330",  "1DTV232", "1234567890123460"),
    ("190BB13000", "1110380",  "1DTV233", "1234567890123461"),
    ("190BC00012", "2825800",  "1DTV234", "1234567890123462"),
    ("190BC00020", "1974175",  "1DTV235", "1234567890123463"),
    ("6440ON6249", "2218590",  "7452009", "1234567890123464"),
    ("484BB05903", "2517060",  "1DTV237", "1234567890123465"),
    ("458LZ00188", "1942275",  "1DTV080", "1234567890123466"),
    # ─── add more rows here ───
]

cols = ["equifax", "experian", "transunion", "consumer_account_number"]
new_ids_flat = spark.createDataFrame(rows, cols)

# Collapse → struct identical to original schema
new_ids_struct = (
    new_ids_flat.select(
        "consumer_account_number",
        F.struct("equifax", "experian", "transunion").alias("identification_number")
    )
)

# ─────────────────────────────────────────────
# 3 ▸ Join & overwrite
# ─────────────────────────────────────────────
df_updated = (
    df_raw.alias("base")
    .join(new_ids_struct.alias("upd"), on="consumer_account_number", how="left")
    .withColumn(
        "identification_number",
        F.coalesce("upd.identification_number", "base.identification_number")
    )
    .drop("upd.identification_number")
)

# ─────────────────────────────────────────────
# 4 ▸ Write back to Parquet
# ─────────────────────────────────────────────
PATH_OUT = "/path/to/updated/accounts.parquet"
(
    df_updated
    .write
    .mode("overwrite")      # or "overwrite" → replace entire folder
    .parquet(PATH_OUT)
)

print("✅   Updated Parquet saved to:", PATH_OUT)
