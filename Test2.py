
import ast
import json
import re


# -------------------------------------------------------------------
# HARDCODE YOUR FILE PATHS HERE
# -------------------------------------------------------------------
AVRO_SCHEMA_PATH = "/Users/vmq634/PycharmProjects/calculator/main/edq/ecbr-card-self-service/avro_schema/customer_information_secondary.avsc"
PYTHON_SCHEMA_PATH = "/Users/vmq634/PycharmProjects/calculator/main/edq/ecbr-card-self-service/ecbr_card_self_service/schemas/customer_information_secondary.py"


# -------------------------------------------------------------------
# TYPE MAPPING
# -------------------------------------------------------------------
PYTHON_TYPE_MAP = {
    "StringType": "string",
    "BooleanType": "boolean",
    "DateType": "date",
    "TimestampType": "timestamp",
    "IntegerType": "int",
    "LongType": "long",
    "FloatType": "float",
    "DoubleType": "double",
    "DecimalType": "decimal",
    "BinaryType": "bytes",
}


def normalize_avro_type(field_type):
    if isinstance(field_type, list):
        non_null_types = [t for t in field_type if t != "null"]
        if len(non_null_types) == 1:
            return normalize_avro_type(non_null_types[0])
        return "|".join(sorted(normalize_avro_type(t) for t in non_null_types))

    if isinstance(field_type, dict):
        logical_type = field_type.get("logicalType")
        base_type = field_type.get("type")

        if logical_type == "date":
            return "date"
        if logical_type in {"timestamp-millis", "timestamp-micros"}:
            return "timestamp"
        if logical_type == "decimal":
            return "decimal"

        return normalize_avro_type(base_type)

    if isinstance(field_type, str):
        return field_type.lower()

    return str(field_type).lower()


def read_avro_schema(avro_schema_path):
    with open(avro_schema_path, "r", encoding="utf-8") as file_pointer:
        avro_json = json.load(file_pointer)

    avro_fields = {}

    for field in avro_json["fields"]:
        field_name = field["name"]
        field_type = field["type"]
        avro_fields[field_name] = normalize_avro_type(field_type)

    return avro_fields


def extract_python_type(annotation_node):
    annotation_text = ast.unparse(annotation_node)

    match = re.search(r"([A-Za-z_][A-Za-z0-9_]*Type)", annotation_text)
    if match:
        raw_type = match.group(1)
        return PYTHON_TYPE_MAP.get(raw_type, raw_type.lower())

    return annotation_text.lower()


def find_schema_class(parsed_tree):
    for node in parsed_tree.body:
        if isinstance(node, ast.ClassDef):
            for base in node.bases:
                if "Schema" in ast.unparse(base):
                    return node

    raise ValueError("No class inheriting from Schema found in Python file.")


def read_python_schema(python_schema_path):
    with open(python_schema_path, "r", encoding="utf-8") as file_pointer:
        python_source = file_pointer.read()

    parsed_tree = ast.parse(python_source)
    schema_class = find_schema_class(parsed_tree)

    python_fields = {}

    for statement in schema_class.body:
        if isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name):
            field_name = statement.target.id
            field_type = extract_python_type(statement.annotation)
            python_fields[field_name] = field_type

    return python_fields


def compare_schemas(avro_fields, python_fields):
    python_not_in_avro = []
    avro_not_in_python = []
    datatype_mismatches = []

    # Python -> missing in Avro
    for field_name, python_type in python_fields.items():
        if field_name not in avro_fields:
            python_not_in_avro.append(field_name)
        else:
            avro_type = avro_fields[field_name]
            if python_type != avro_type:
                datatype_mismatches.append((field_name, python_type, avro_type))

    # Avro -> missing in Python
    for field_name in avro_fields:
        if field_name not in python_fields:
            avro_not_in_python.append(field_name)

    return python_not_in_avro, avro_not_in_python, datatype_mismatches


def main():
    avro_fields = read_avro_schema(AVRO_SCHEMA_PATH)
    python_fields = read_python_schema(PYTHON_SCHEMA_PATH)

    python_not_in_avro, avro_not_in_python, datatype_mismatches = compare_schemas(
        avro_fields, python_fields
    )

    print("=" * 80)
    print("FIELDS PRESENT IN PYTHON SCHEMA BUT NOT PRESENT IN AVRO SCHEMA")
    print("=" * 80)
    print(f"Count: {len(python_not_in_avro)}")
    for field_name in python_not_in_avro:
        print(field_name)

    print()
    print("=" * 80)
    print("FIELDS PRESENT IN AVRO SCHEMA BUT NOT PRESENT IN PYTHON SCHEMA")
    print("=" * 80)
    print(f"Count: {len(avro_not_in_python)}")
    for field_name in avro_not_in_python:
        print(field_name)

    print()
    print("=" * 80)
    print("DATATYPE MISMATCHES")
    print("=" * 80)
    print(f"Count: {len(datatype_mismatches)}")
    for field_name, python_type, avro_type in datatype_mismatches:
        print(f"{field_name} -> Python: {python_type} | Avro: {avro_type}")


if __name__ == "__main__":
    main()
