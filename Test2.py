
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType

# Assuming df is your DataFrame
df = df.withColumn("business_date", F.to_date(F.col("business_date"))) \
       .withColumn("run_identifier", F.col("run_identifier").cast(IntegerType()))

# Now you can check the schema
df.printSchema()

# To see the changes applied
df.show()
