
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql.window import Window

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

# Define the window specification with partitioning
windowSpec = Window.orderBy(monotonically_increasing_id()).partitionBy("dummy")

# Add a dummy column to use for partitioning
df1 = df1.withColumn("dummy", lit(1))
df2 = df2.withColumn("dummy", lit(1))

# Add an index to both DataFrames using row_number() and partitioning
df1 = df1.withColumn("index", row_number().over(windowSpec)).drop("dummy")
df2 = df2.withColumn("index", row_number().over(windowSpec)).drop("dummy")

# Join DataFrames on the index column
df3 = df2.join(df1.select("a", "index"), on="index", how="inner").drop("index")

# Show the result DataFrame
df3.show(truncate=False)
