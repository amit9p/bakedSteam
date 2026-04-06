import os
import json

# Input folder containing original JSON files
input_folder = "/path/to/input/rules"

# Output folder where updated JSON files will be written
output_folder = "/path/to/output/rules_swapped"

# Create output folder if it does not exist
os.makedirs(output_folder, exist_ok=True)


def swap_suppression_tags(input_file_path, output_file_path):
    try:
        # Read original JSON
        with open(input_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Check if "rules" section exists
        if "rules" in data:
            rules = data["rules"]

            # Read both keys safely
            non_suppressed = rules.get("non_suppressed", [])
            suppressed = rules.get("suppressed", [])

            # Swap values
            rules["non_suppressed"] = suppressed
            rules["suppressed"] = non_suppressed

        # Write updated JSON to output folder
        with open(output_file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        print(f"Created: {output_file_path}")

    except Exception as e:
        print(f"Error processing {input_file_path}: {e}")


# Process all JSON files from input folder
for filename in os.listdir(input_folder):
    if filename.endswith(".json"):
        input_file_path = os.path.join(input_folder, filename)
        output_file_path = os.path.join(output_folder, filename)

        swap_suppression_tags(input_file_path, output_file_path)

print("Done 👍")
