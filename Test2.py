
import json

file_path = "input.json"  # Replace with your file path

try:
    with open(file_path, "r") as file:
        json.load(file)
    print("Valid JSON ✅")
except json.JSONDecodeError as e:
    print(f"Invalid JSON ❌: {e}")
