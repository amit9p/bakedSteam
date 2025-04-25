
import os
import json

# Get current directory (where script is located)
current_dir = os.path.dirname(os.path.abspath(__file__))

# Build full path to the JSON file
json_file_path = os.path.join(current_dir, "final_output.json")

# Read JSON file
with open(json_file_path, "r") as f:
    data = json.load(f)

# Now 'data' is a Python list/dict from the JSON
print(data)





import os
import shutil

# Step 1: Write the JSON as a single file into a temporary folder
current_dir = os.path.dirname(os.path.abspath(__file__))
temp_output_path = os.path.join(current_dir, "temp_output_json")

result_df.coalesce(1).write.mode("overwrite").json(temp_output_path)

# Step 2: Move the single JSON file out and clean up
# Find the part file
for file_name in os.listdir(temp_output_path):
    if file_name.startswith("part-") and file_name.endswith(".json"):
        source_file = os.path.join(temp_output_path, file_name)
        final_output_file = os.path.join(current_dir, "final_output.json")
        shutil.move(source_file, final_output_file)

# Step 3: Delete the temp output folder
shutil.rmtree(temp_output_path)
