

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col, explode_outer

spark = SparkSession.builder.getOrCreate()

# ---- paths ----
json_input      = "s3://your-bucket/input/*.json"       # or "/path/to/input/*.json"
tmp_csv_folder  = "s3://your-bucket/tmp_json_to_csv"     # staging folder Spark writes to
final_csv_file  = "s3://your-bucket/output/data.csv"     # desired SINGLE csv file path

# 1) Read JSON (supports pretty/multiline files)
df = spark.read.option("multiline", "true").json(json_input)

# 2) Flatten nested data (explode arrays; expand structs)
def flatten_df(frame):
    def complex_cols(f):
        return [ (fld.name, fld.dataType) for fld in f.schema.fields
                 if isinstance(fld.dataType, (StructType, ArrayType)) ]

    pending = dict(complex_cols(frame))
    while pending:
        name, dtype = next(iter(pending.items()))
        if isinstance(dtype, StructType):
            # expand "name.*" into top-level columns "name_child"
            expansions = [col(f"{name}.{c}").alias(f"{name}_{c}") for c in dtype.names]
            frame = frame.select([c for c in frame.columns if c != name] + expansions)
        else:
            # turn array into multiple rows (keeps nulls)
            frame = frame.withColumn(name, explode_outer(name))
        pending = dict(complex_cols(frame))
    return frame

df_flat = flatten_df(df)

# 3) Write a SINGLE CSV part with header (to a temp folder)
(df_flat
 .coalesce(1)                             # force a single output file
 .write
 .option("header", "true")
 .mode("overwrite")
 .csv(tmp_csv_folder))

# 4) Rename that single part-* file to your final name, then remove the temp folder
hadoop = spark._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop)
Path = spark._jvm.org.apache.hadoop.fs.Path
for f in fs.listStatus(Path(tmp_csv_folder)):
    name = f.getPath().getName()
    if name.startswith("part-") and name.endswith(".csv"):
        fs.rename(f.getPath(), Path(final_csv_file))
        break
fs.delete(Path(tmp_csv_folder), True)
