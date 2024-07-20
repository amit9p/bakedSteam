
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrame Column Merge Example").getOrCreate()

# Sample data for df1
data1 = [
    ("a1", "b1", "c1"),
    ("a2", "b2", "c2"),
    ("a3", "b3", "c3")
]

columns1 = ["a", "b", "c"]

df1 = spark.createDataFrame(data1, columns1)

# Sample data for df2
data2 = [
    ("d1", "e1", "f1"),
    ("d2", "e2", "f2"),
    ("d3", "e3", "f3")
]

columns2 = ["d", "e", "f"]

df2 = spark.createDataFrame(data2, columns2)

# Add an index to both DataFrames using monotonically_increasing_id
df1 = df1.withColumn("id", monotonically_increasing_id())
df2 = df2.withColumn("id", monotonically_increasing_id())

# Join DataFrames on the id column
df3 = df2.join(df1.select("a", "id"), on="id", how="inner").drop("id")

# Show the result DataFrame
df3.show(truncate=False)
