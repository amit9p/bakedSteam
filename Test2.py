
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("replace-identification-number-22rows")
    .getOrCreate()
)

# ── 1.  Original Parquet ───────────────────────────────────────────
src_path = "/path/or/s3/in/accounts_original.parquet"   # ← change
df_raw   = spark.read.parquet(src_path)

# ── 2.  22-row mapping table  ──────────────────────────────────────
rows = [
    # (consumer_account_number ,   Equifax , Experian , TransUnion)
    (1 ,  "850BB01498", "1270246", "1DTV001"),
    (2 ,  "484BB01456", "1205950", "1DTV003"),
    (3 ,  "190BB12984", "1109050", "1DTV228"),
    (4 ,  "190BB13037", "1110080", "1DTV229"),
    (5 ,  "190BB13028", "1110330", "1DTV232"),
    (6 ,  "190BB13000", "1110380", "1DTV233"),
    (7 ,  "190BC00012", "2825800", "1DTV234"),
    (8 ,  "190BC00020", "1974175", "1DTV235"),
    (9 ,  "6440ON6249", "2218590", "7452009"),
    (10,  "484BB05903", "2517060", "1DTV237"),
    (11,  "458LZ00188", "1942275", "1DTV080"),
    (12,  "163BB34197", "1949299", "1DTV202"),
    (13,  "155DC03627", "1195162", "2DQ3001"),
    (14,  "190BB12831", "2445250", "1DTV225"),
    (15,  "155DC03411", "1195324", "2DQ2001"),
    (16,  "155DC03429", "1195328", "1DTV040"),
    (17,  "484BB06174", "2926315", "1DTV238"),
    (18,  "484BB06299", "2990785", "1DTV241"),
    (19,  "163BB34160", "1201640", "1DTV041"),
    (20,  "163BB34179", "1949295", "1DTV057"),
    (21,  "155DC03445", "1942066", "1DTV075"),
    (22,  "484BB06358", "3947032", "1DTV244"),
]

schema = (
    "consumer_account_number INT, "
    "equifax STRING, experian STRING, transunion STRING"
)
new_ids_flat = spark.createDataFrame(rows, schema)

# ── 3.  Build the replacement struct  ──────────────────────────────
new_ids_struct = (
    new_ids_flat.select(
        "consumer_account_number",
        F.struct(
            F.col("equifax"),
            F.col("experian"),
            F.col("transunion")
        ).alias("identification_number")
    )
)

# ── 4.  Swap the struct where account_number 1-22 match ────────────
df_updated = (
    df_raw.alias("base")
    .join(new_ids_struct.alias("u"), on="consumer_account_number", how="left")
    .withColumn(
        "identification_number",
        F.coalesce("u.identification_number", "base.identification_number")
    )
    .drop("u.identification_number")
)

# ── 5.  Write out the refreshed Parquet  ───────────────────────────
dst_path = "/path/or/s3/out/accounts_fixed.parquet"     # ← change
df_updated.write.mode("overwrite").parquet(dst_path)

print("✅  Parquet with updated identification_number written to:", dst_path)
