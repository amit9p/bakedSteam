

^\d{10}$
^[a-zA-Z0-9]{9}$

^[B0-6DL]{24}$



^\d{2}$

^\d{4}$
^[a-zA-Z0-9]{16}$


import json

file_path = "input.json"  # Replace with your file path

try:
    with open(file_path, "r") as file:
        json.load(file)
    print("Valid JSON ✅")
except json.JSONDecodeError as e:
    print(f"Invalid JSON ❌: {e}")
