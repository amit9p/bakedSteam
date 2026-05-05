
import re
from pathlib import Path


# Hardcode your 5 schema files here
schema_files = [
    "ab_segment.py",
    "ad_segment.py",
    "cl_segment.py",
    "is_segment.py",
    "ti_segment.py",
]

# Output file name
output_file = "unified_schema.py"

# Conflict report file
conflict_file = "schema_conflicts.txt"


# This pattern finds lines like:
# account_id: Column[StringType]
field_pattern = re.compile(
    r"^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:\s*Column\[(.*?)\](\s*#.*)?$"
)

# Dictionary to store unique fields
# Example: {"account_id": "StringType"}
unique_fields = {}

# This keeps field comments if present
field_comments = {}

# This keeps datatype conflicts
conflicts = []


# Loop through each schema file
for schema_file in schema_files:
    file_path = Path(schema_file)

    # Read the full file as text
    file_lines = file_path.read_text().splitlines()

    # Loop through each line in the file
    for line in file_lines:
        match = field_pattern.match(line)

        # Skip lines which are not schema field definitions
        if not match:
            continue

        # Extract field name
        field_name = match.group(1)

        # Extract datatype, example StringType / IntegerType / DateType
        field_type = match.group(2).strip()

        # Extract comment if present
        field_comment = match.group(3) or ""

        # If field is not already added, add it
        if field_name not in unique_fields:
            unique_fields[field_name] = field_type
            field_comments[field_name] = field_comment

        # If same field exists with different datatype, capture conflict
        elif unique_fields[field_name] != field_type:
            conflicts.append(
                f"{field_name}: first type = {unique_fields[field_name]}, "
                f"conflicting type in {schema_file} = {field_type}"
            )


# Collect all datatypes used in final schema
used_types = sorted(set(unique_fields.values()))

# Create import line dynamically
type_imports = ", ".join(used_types)


# Start writing unified schema file content
output_lines = []

output_lines.append(f"from pyspark.sql.types import {type_imports}")
output_lines.append("from typedspark import Column, Schema")
output_lines.append("")
output_lines.append("")
output_lines.append("class UnifiedSchema(Schema):")

# Add all unique fields into the unified schema
for field_name, field_type in unique_fields.items():
    comment = field_comments.get(field_name, "")
    output_lines.append(f"    {field_name}: Column[{field_type}]{comment}")

# Write unified_schema.py
Path(output_file).write_text("\n".join(output_lines) + "\n")

# Write conflicts, if any
if conflicts:
    Path(conflict_file).write_text("\n".join(conflicts) + "\n")
else:
    Path(conflict_file).write_text("No datatype conflicts found.\n")


print(f"Unified schema generated: {output_file}")
print(f"Total unique fields: {len(unique_fields)}")
print(f"Conflict report generated: {conflict_file}")
