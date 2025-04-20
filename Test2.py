
from pyspark.sql import SparkSession
import requests
import json

# Step 1: Call the API
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json;v=1",
    "Authorization": "Bearer your_token_here"
}
url = base_url + result_api_url + "?jobId=" + job_id + "&datasetConfigurationId=" + datasetConfigurationId
response = requests.get(url, headers=headers, verify=False)

# Step 2: Convert API response to JSON
json_data = json.loads(response.text)

# Step 3: Start Spark session
spark = SparkSession.builder.appName("ParseJSON").getOrCreate()

# Step 4: Parallelize and create DataFrame
rdd = spark.sparkContext.parallelize([json_data])
df = spark.read.json(rdd)

# Step 5: Print schema
df.printSchema()

# Optional: Show a sample
df.show(truncate=False)

# If you want to extract specific fields (e.g. "jobId", "status"), use:
# df.select("jobId", "status").show()
