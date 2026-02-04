
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
    sc._jsc.hadoopConfiguration()
)

path = sc._jvm.org.apache.hadoop.fs.Path("s3://my-bucket/input/")

files = fs.listStatus(path)

for f in files:
    print(f.getPath().toString(), f.getLen())


