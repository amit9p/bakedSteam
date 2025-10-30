2014-02-14T00:00:00.000




from pyspark.sql.functions import col, to_date, coalesce, date_format, lit

# Choose the final format you want in the CSV
FINAL_FMT = "yyyy-MM-dd"     # change to what your receiver expects (e.g. "MM/dd/yyyy")

def norm_date(c):
    # try several common inputs; add more if needed
    return coalesce(
        to_date(col(c), "yyyy-MM-dd"),
        to_date(col(c), "MM/dd/yyyy"),
        to_date(col(c), "yyyyMMdd"),
        to_date(col(c))  # fallback: Spark’s best-effort
    )

df_out = (df
    .withColumn("transaction_date",          date_format(norm_date("transaction_date"), FINAL_FMT))
    .withColumn("transaction_posting_date",  date_format(norm_date("transaction_posting_date"), FINAL_FMT))
)

(df_out
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .option("dateFormat", FINAL_FMT)           # for Date columns
 .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssXXX")  # if you have timestamps
 .csv("s3://.../tmp"))
