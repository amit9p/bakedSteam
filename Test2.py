
def read_with_credentials(spark, access_key, secret_key, session_token, s3_path):
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", access_key)
    hadoop_conf.set("fs.s3a.secret.key", secret_key)
    hadoop_conf.set("fs.s3a.session.token", session_token)
    return spark.read.parquet(s3_path)

# Example
spark = SparkSession.builder.appName("SingleSpark").getOrCreate()

df1 = read_with_credentials(spark, access_key1, secret_key1, session_token1, "s3a://bucket1/path")
df2 = read_with_credentials(spark, access_key2, secret_key2, session_token2, "s3a://bucket2/path")

df1.show()
df2.show()

spark.stop()
