
import os

# Get the directory where the current script is located
current_dir = os.path.dirname(os.path.abspath(__file__))

# Define output path
output_path = os.path.join(current_dir, "output_json")

# Write DataFrame as a single JSON file
result_df.coalesce(1).write.mode("overwrite").json(output_path)
