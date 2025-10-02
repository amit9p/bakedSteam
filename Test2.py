
from pyspark.sql.functions import col
from pyspark.sql.types import LongType, StringType, DateType

df_converted = (
    df
    .withColumn("customer_pk_id", col("customer_pk_id").cast(LongType()))
    .withColumn("customer_id", col("customer_id").cast(LongType()))
    .withColumn("account_id", col("account_id").cast(LongType()))
    .withColumn("consumer_info_indicator", col("consumer_info_indicator").cast(StringType()))
    .withColumn("ownership_type_ecoa", col("ownership_type_ecoa").cast(StringType()))
    .withColumn("reporting_status", col("reporting_status").cast(StringType()))
    .withColumn("reporting_reason", col("reporting_reason").cast(StringType()))
    .withColumn("snap_dt", col("snap_dt").cast(DateType()))
    .withColumn("edw_publn_id", col("edw_publn_id").cast(LongType()))
    .withColumn("instnc_id", col("instnc_id").cast(StringType()))
)
