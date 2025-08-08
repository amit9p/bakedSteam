
from pyspark.sql import functions as F

# df1 = big table (image 1)
# df2 = small table (image 2) -> has cols: consumer_account_number, identification_number (struct)

# 1) make sure join key dtypes match
key_dtype = dict(df1.dtypes)["consumer_account_number"]
df2_k = df2.select(
    F.col("consumer_account_number").cast(key_dtype).alias("consumer_account_number"),
    F.col("identification_number").alias("identification_number_new")  # rename BEFORE join
)

# 2) join with aliases and overwrite the struct
df_updated = (
    df1.alias("l")
      .join(df2_k.alias("r"), on="consumer_account_number", how="left")
      .withColumn(
          "identification_number",
          F.coalesce(F.col("r.identification_number_new"), F.col("l.identification_number"))
      )
      .drop("r.identification_number_new")
)

df_updated.select("consumer_account_number","identification_number").show(truncate=False)


*********
from pyspark.sql import functions as F

# df1  = big dataframe (image 1) → has many columns incl. "identification_number" (struct)
# df2  = small dataframe (image 2) → cols: "consumer_account_number", "identification_number" (struct)

# 1) Make sure join keys have the same dtype
key_dtype = dict(df1.dtypes)["consumer_account_number"]  # e.g. 'string' or 'int'

df2_fix = df2.select(
    F.col("consumer_account_number").cast(key_dtype).alias("consumer_account_number"),
    F.col("identification_number").alias("identification_number_new")
)

# 2) Join and replace the struct where there’s a match
df_updated = (
    df1.join(df2_fix, on="consumer_account_number", how="left")
       .withColumn(
           "identification_number",
           F.coalesce(F.col("identification_number_new"), F.col("identification_number"))
       )
       .drop("identification_number_new")
)

# (optional) sanity check
df_updated.select("consumer_account_number", "identification_number").show(truncate=False)

# (optional) write back
# df_updated.write.mode("overwrite").parquet("/path/out/accounts_updated.parquet")

____
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("ident_numbers_with_accno").getOrCreate()

# Build rows on the JVM (no Python inference), now with a 4th column: consumer_account_number 1..22
base = spark.sql("""
SELECT * FROM VALUES
 ('850BB01498','1270246','1DTV001', 1),
 ('484BB01456','1205950','1DTV003', 2),
 ('190BB12984','1109050','1DTV228', 3),
 ('190BB13037','1110080','1DTV229', 4),
 ('190BB13028','1110330','1DTV232', 5),
 ('190BB13000','1110380','1DTV233', 6),
 ('190BC00012','2825800','1DTV234', 7),
 ('190BC00020','1974175','1DTV235', 8),
 ('6440ON6249','2218590','7452009', 9),
 ('484BB05903','2517060','1DTV237',10),
 ('458LZ00188','1942275','1DTV080',11),
 ('163BB34197','1949299','1DTV202',12),
 ('155DC03627','1195162','2DQ3001',13),
 ('190BB12831','2445250','1DTV225',14),
 ('155DC03411','1195324','2DQ2001',15),
 ('155DC03429','1195328','1DTV040',16),
 ('484BB06174','2926315','1DTV238',17),
 ('484BB06299','2990785','1DTV241',18),
 ('163BB34160','1201640','1DTV041',19),
 ('163BB34179','1949295','1DTV057',20),
 ('155DC03445','1942066','1DTV075',21),
 ('484BB06358','3947032','1DTV244',22)
AS t(equifax, experian, transunion, consumer_account_number)
""")

# Pack into struct + keep the account number
df_out = base.select(
    F.struct("equifax","experian","transunion").alias("identification_number"),
    # cast to STRING if your target schema expects string; change to "int" if needed
    F.col("consumer_account_number").cast("string").alias("consumer_account_number")
)

df_out.printSchema()
df_out.show(truncate=False)

# Optional write
# df_out.write.mode("overwrite").parquet("/tmp/ident22_with_acc.parquet")


<><><><><>
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("ident_numbers_sql_values")
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")  # not required, just extra-safe
    .getOrCreate()
)

# Build the table in SQL (JVM side) — no Python list → no inference/pickling
triples_df = spark.sql("""
SELECT * FROM VALUES
 ('850BB01498','1270246','1DTV001'),
 ('484BB01456','1205950','1DTV003'),
 ('190BB12984','1109050','1DTV228'),
 ('190BB13037','1110080','1DTV229'),
 ('190BB13028','1110330','1DTV232'),
 ('190BB13000','1110380','1DTV233'),
 ('190BC00012','2825800','1DTV234'),
 ('190BC00020','1974175','1DTV235'),
 ('6440ON6249','2218590','7452009'),
 ('484BB05903','2517060','1DTV237'),
 ('458LZ00188','1942275','1DTV080'),
 ('163BB34197','1949299','1DTV202'),
 ('155DC03627','1195162','2DQ3001'),
 ('190BB12831','2445250','1DTV225'),
 ('155DC03411','1195324','2DQ2001'),
 ('155DC03429','1195328','1DTV040'),
 ('484BB06174','2926315','1DTV238'),
 ('484BB06299','2990785','1DTV241'),
 ('163BB34160','1201640','1DTV041'),
 ('163BB34179','1949295','1DTV057'),
 ('155DC03445','1942066','1DTV075'),
 ('484BB06358','3947032','1DTV244')
AS t(equifax, experian, transunion)
""")

# Pack into the struct column exactly like your schema
df_struct = triples_df.select(
    F.struct("equifax", "experian", "transunion").alias("identification_number")
)

df_struct.printSchema()
df_struct.show(truncate=False)

# Write if you need a Parquet output
# df_struct.write.mode("overwrite").parquet("/tmp/ident22.parquet")
^^^^^^
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("no-legacy-infer").getOrCreate()

# ---- 22 triples (all strings) ----
rows = [
    ("850BB01498","1270246","1DTV001"),
    ("484BB01456","1205950","1DTV003"),
    ("190BB12984","1109050","1DTV228"),
    ("190BB13037","1110080","1DTV229"),
    ("190BB13028","1110330","1DTV232"),
    ("190BB13000","1110380","1DTV233"),
    ("190BC00012","2825800","1DTV234"),
    ("190BC00020","1974175","1DTV235"),
    ("6440ON6249","2218590","7452009"),
    ("484BB05903","2517060","1DTV237"),
    ("458LZ00188","1942275","1DTV080"),
    ("163BB34197","1949299","1DTV202"),
    ("155DC03627","1195162","2DQ3001"),
    ("190BB12831","2445250","1DTV225"),
    ("155DC03411","1195324","2DQ2001"),
    ("155DC03429","1195328","1DTV040"),
    ("484BB06174","2926315","1DTV238"),
    ("484BB06299","2990785","1DTV241"),
    ("163BB34160","1201640","1DTV041"),
    ("163BB34179","1949295","1DTV057"),
    ("155DC03445","1942066","1DTV075"),
    ("484BB06358","3947032","1DTV244"),
]

# ---- explicit schema (no inference) ----
triples_schema = StructType([
    StructField("equifax",    StringType(), True),
    StructField("experian",   StringType(), True),
    StructField("transunion", StringType(), True),
])

# ---- build DF from RDD with explicit schema (positional arg) ----
rdd = spark.sparkContext.parallelize(rows)
triples_df = spark.createDataFrame(rdd, triples_schema)

# ---- pack into the struct column you need ----
df_struct = triples_df.select(
    F.struct("equifax","experian","transunion").alias("identification_number")
)

df_struct.printSchema()
df_struct.show(truncate=False)

# (optional) write
# df_struct.write.mode("overwrite").parquet("/tmp/ident22.parquet")

_
______
from pyspark.sql import SparkSession, Row, functions as F, types as T

spark = (
    SparkSession.builder
    .appName("ident-number-df")
    .getOrCreate()
)

# ── raw triples exactly as in the sheet (image-2) ────────────────
rows = [
    ("850BB01498", "1270246",  "1DTV001"),
    ("484BB01456", "1205950",  "1DTV003"),
    ("190BB12984", "1109050",  "1DTV228"),
    ("190BB13037", "1110080",  "1DTV229"),
    ("190BB13028", "1110330",  "1DTV232"),
    ("190BB13000", "1110380",  "1DTV233"),
    ("190BC00012", "2825800",  "1DTV234"),
    ("190BC00020", "1974175",  "1DTV235"),
    ("6440ON6249", "2218590",  "7452009"),
    ("484BB05903", "2517060",  "1DTV237"),
    ("458LZ00188", "1942275",  "1DTV080"),
    ("163BB34197", "1949299",  "1DTV202"),
    ("155DC03627", "1195162",  "2DQ3001"),
    ("190BB12831", "2445250",  "1DTV225"),
    ("155DC03411", "1195324",  "2DQ2001"),
    ("155DC03429", "1195328",  "1DTV040"),
    ("484BB06174", "2926315",  "1DTV238"),
    ("484BB06299", "2990785",  "1DTV241"),
    ("163BB34160", "1201640",  "1DTV041"),
    ("163BB34179", "1949295",  "1DTV057"),
    ("155DC03445", "1942066",  "1DTV075"),
    ("484BB06358", "3947032",  "1DTV244")
]

# ── create an intermediate DF with three simple string columns ──
triples_df = spark.createDataFrame(rows, ["equifax", "experian", "transunion"])

# ── pack them into the struct & keep ONLY that struct column ─────
df_struct = (
    triples_df
    .withColumn("identification_number",
                F.struct("equifax", "experian", "transunion"))
    .select("identification_number")
)

df_struct.printSchema()
df_struct.show(truncate=False)


__________
from pyspark.sql import SparkSession

# 1️⃣  session
spark = SparkSession.builder.appName("check").getOrCreate()

# 2️⃣  dummy rows
rows = [
    (1, "E1", "X1", "T1"),
    (2, "E2", "X2", "T2")
]

# 3️⃣  create DF  (DON’T overwrite `spark`!)
df = spark.createDataFrame(rows) \
         .toDF("consumer_account_number", "equifax", "experian", "transunion")

df.show()

---
from pyspark.sql import SparkSession

# 1️⃣  session
spark = SparkSession.builder.appName("check").getOrCreate()

# 2️⃣  dummy rows
rows = [
    (1, "E1", "X1", "T1"),
    (2, "E2", "X2", "T2")
]

# 3️⃣  create DF  (DON’T overwrite `spark`!)
df = spark.createDataFrame(rows) \
         .toDF("consumer_account_number", "equifax", "experian", "transunion")

df.show()

-----
rows = [
    (1 , "850BB01498", "1270246", "1DTV001"),
    (2 , "484BB01456", "1205950", "1DTV003"),
    # …
]
new_ids_flat = spark.createDataFrame(rows).toDF(
    "consumer_account_number", "equifax", "experian", "transunion"
)


spark = (
    SparkSession.builder
    .appName("replace-identification-number-22rows")
    .getOrCreate()
)


from pyspark.sql import Row

rows = [
    Row(consumer_account_number=1 , equifax="850BB01498", experian="1270246", transunion="1DTV001"),
    Row(consumer_account_number=2 , equifax="484BB01456", experian="1205950", transunion="1DTV003"),
    # … add the remaining 20 rows …
]

ddl = """
consumer_account_number INT,
equifax                 STRING,
experian                STRING,
transunion              STRING
"""

# CAPITAL ‘D’ in createDataFrame
new_ids_flat = spark.createDataFrame(rows, ddl)


-----
from pyspark.sql import SparkSession, Row, functions as F

spark = (
    SparkSession.builder
    .appName("replace-identification-number-22rows")
    .getOrCreate()
)

# ─────────────────────────────────────────────────────────
# 1. Original Parquet
# ─────────────────────────────────────────────────────────
src_path = "/path/in/accounts_original.parquet"
df_raw   = spark.read.parquet(src_path)

# ─────────────────────────────────────────────────────────
# 2.  EXACT 22-row mapping  —  every value forced to string
# ─────────────────────────────────────────────────────────
rows_py = [
    (1 , "850BB01498", "1270246", "1DTV001"),
    (2 , "484BB01456", "1205950", "1DTV003"),
    (3 , "190BB12984", "1109050", "1DTV228"),
    (4 , "190BB13037", "1110080", "1DTV229"),
    (5 , "190BB13028", "1110330", "1DTV232"),
    (6 , "190BB13000", "1110380", "1DTV233"),
    (7 , "190BC00012", "2825800", "1DTV234"),
    (8 , "190BC00020", "1974175", "1DTV235"),
    (9 , "6440ON6249", "2218590", "7452009"),
    (10, "484BB05903", "2517060", "1DTV237"),
    (11, "458LZ00188", "1942275", "1DTV080"),
    (12, "163BB34197", "1949299", "1DTV202"),
    (13, "155DC03627", "1195162", "2DQ3001"),
    (14, "190BB12831", "2445250", "1DTV225"),
    (15, "155DC03411", "1195324", "2DQ2001"),
    (16, "155DC03429", "1195328", "1DTV040"),
    (17, "484BB06174", "2926315", "1DTV238"),
    (18, "484BB06299", "2990785", "1DTV241"),
    (19, "163BB34160", "1201640", "1DTV041"),
    (20, "163BB34179", "1949295", "1DTV057"),
    (21, "155DC03445", "1942066", "1DTV075"),
    (22, "484BB06358", "3947032", "1DTV244"),
]

# Build a real Row list – the safest way
rows = [Row(acc, eq, ex, tu) for acc, eq, ex, tu in rows_py]

ddl = """
consumer_account_number INT,
equifax                 STRING,
experian                STRING,
transunion              STRING
"""

new_ids_flat = spark.createDataFrame(rows, ddl)

# ─────────────────────────────────────────────────────────
# 3. Collapse into struct
# ─────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────
# 4. Join & overwrite
# ─────────────────────────────────────────────────────────
df_updated = (
    df_raw.alias("base")
    .join(new_ids_struct.alias("u"), on="consumer_account_number", how="left")
    .withColumn(
        "identification_number",
        F.coalesce("u.identification_number", "base.identification_number")
    )
    .drop("u.identification_number")
)

# ─────────────────────────────────────────────────────────
# 5. Write back to Parquet
# ─────────────────────────────────────────────────────────
dst_path = "/path/out/accounts_fixed.parquet"
df_updated.write.mode("overwrite").parquet(dst_path)

print("✅ 22 identification_number structs updated:", dst_path)
