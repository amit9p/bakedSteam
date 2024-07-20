
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrame Column Merge Example").getOrCreate()

# Sample data for df1
data1 = [
    (1, "a1", "b1", "c1"),
    (2, "a2", "b2", "c2"),
    (3, "a3", "b3", "c3")
]

columns1 = ["id", "a", "b", "c"]

df1 = spark.createDataFrame(data1, columns1)

# Sample data for df2
data2 = [
    ("d1", "e1", "f1"),
    ("d2", "e2", "f2"),
    ("d3", "e3", "f3")
]

columns2 = ["d", "e", "f"]

df2 = spark.createDataFrame(data2, columns2)

# Show the original DataFrames
print("DataFrame 1:")
df1.show()

print("DataFrame 2:")
df2.show()

# Select the column 'a' from df1
df1_a = df1.select("a")

# Add the column 'a' from df1 to df2
df3 = df2.withColumn("a", lit(df1_a.collect()[0][0]))

# Show the result DataFrame
print("Result DataFrame 3:")
df3.show()
