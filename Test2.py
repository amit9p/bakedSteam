

from pyspark.sql import functions as F

# Assuming your DataFrame is df_fs
df_primary = df_fs.filter(F.col("account_type") == "PRIMARY")

# Show few rows to verify
df_primary.show(20, truncate=False)

# If you want to save results as CSV
df_primary.coalesce(1).write.format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save("/Users/vmq634/PyCharmProjects/calculator/main/output/primary_accounts")




from pyspark.sql import functions as F

STRUCT_COLS = [c for c, t in df_fs.dtypes if t.startswith("struct")]

df_out = (
    df_fs
      .drop(*STRUCT_COLS)       # remove all struct columns (e.g., sdp4_metadata)
      .limit(1000)              # keep first 1000 rows
)

df_out.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv("/path/to/out/folder")


______




# --- Spark & Hadoop conf are already set (fs.s3a.* etc.) ---

sc = spark.sparkContext
hconf = sc._jsc.hadoopConfiguration()

# (Optional) make sure these are present if not set earlier:
hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hconf.set("fs.s3a.endpoint", "s3.amazonaws.com")
hconf.set("fs.s3a.path.style.access", "true")
hconf.set("fs.s3a.access.key", AccessKeyId)
hconf.set("fs.s3a.secret.key", SecretAccessKey)
hconf.set("fs.s3a.session.token", SessionToken)

jvm = sc._jvm
Path = jvm.org.apache.hadoop.fs.Path
URI  = jvm.java.net.URI
FileSystem = jvm.org.apache.hadoop.fs.FileSystem

s3_path = Path("s3a://cof-uscag3-lossmitigation-cat3-qa-useast1/psxlj/lake/recoveries/credit_bureau_reporting_service_credit_bureau_account/src/")

# ✅ bind to S3 by using the path’s URI
fs = FileSystem.get(s3_path.toUri(), hconf)

# Now list subdirs
stats = fs.listStatus(s3_path)
dirs = [st.getPath().getName() for st in stats if st.isDirectory()]
print(dirs)
