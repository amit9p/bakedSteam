
from pyspark.sql.functions import lit

df_updated = df.withColumn("identification_number", lit("CAPDISC"))

unique_account_ids = df.select("account_id").distinct()

df_renamed = (
    df
    .withColumnRenamed("run_id", "ecbr_run_id")
    .withColumnRenamed("run_id_utc_timestamp", "ecbr_run_id_utc_timestamp")
    .withColumnRenamed("run_id_date", "ecbr_run_id_date")
)



from pyspark.sql.functions import lit

# Replace all values in 'identification_number' column with 'CAPDISK'
df_updated = df.withColumn("identification_number", lit("CAPDISK"))


from pyspark.sql.functions import col
from pyspark.sql.types import LongType, StringType, TimestampType, DateType

df_converted = (
    df
    .withColumn("override_id", col("override_id").cast(LongType()))
    .withColumn("account_id", col("account_id").cast(LongType()))
    .withColumn("reporting_status", col("reporting_status").cast(StringType()))
    .withColumn("reporting_reason", col("reporting_reason").cast(StringType()))
    .withColumn("initiated_date", col("initiated_date").cast(TimestampType()))
    .withColumn("initiated_by", col("initiated_by").cast(StringType()))
    .withColumn("closed_date", col("closed_date").cast(TimestampType()))
    .withColumn("closed_by", col("closed_by").cast(StringType()))
    .withColumn("source", col("source").cast(StringType()))
    .withColumn("source_uri", col("source_uri").cast(StringType()))
    .withColumn("snap_dt", col("snap_dt").cast(DateType()))
    .withColumn("edw_publn_id", col("edw_publn_id").cast(LongType()))
    .withColumn("instnc_id", col("instnc_id").cast(StringType()))
)
