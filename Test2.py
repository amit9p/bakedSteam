
from pyspark.sql import functions as F

# Explode the array column to rows
exploded_df = df.select(F.explode(df.output_filetype).alias("filetype_element"))

# Get distinct values
distinct_filetypes = exploded_df.distinct()

# Show the result
distinct_filetypes.show()
