
import json
import csv

input_file = "input.json"
output_file = "rules_output.csv"

# Load JSON
with open(input_file, "r", encoding="utf-8") as f:
    data = json.load(f)

rules_list = data.get("rulesList", [])

with open(output_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    # Header
    writer.writerow(["fieldName", "ruleType", "ruleDetails"])

    for rule in rules_list:
        field_name = rule.get("fieldName", "")
        rule_type = rule.get("ruleType", "")
        rule_detail = rule.get("ruleDetail", {})

        rule_details_str = ""

        # -------- REGEX --------
        if rule_type == "REGEX":
            rule_details_str = rule_detail.get("ruleRegexPattern", "")

        # -------- DATE RANGE --------
        elif rule_type == "DATE_RANGE":
            date_range = rule_detail.get("dateRange", {})

            lower = date_range.get("lowerDateBound", {}).get("date", "")
            upper = date_range.get("upperDateBound", {}).get("date", "")

            rule_details_str = f"lowerDate={lower}, upperDate={upper}"

        # -------- INTEGER / VALUE RANGE --------
        elif rule_type in ["INTEGER_RANGE", "VALUE_RANGE"]:
            value_range = rule_detail.get("valueRange", {})

            lower = value_range.get("lowerThresholdBound", {}).get("boundValue", "")
            upper = value_range.get("upperThresholdBound", {}).get("boundValue", "")

            rule_details_str = f"lowerValue={lower}, upperValue={upper}"

        # -------- VALID VALUES --------
        elif rule_type == "VALID_VALUES":
            values = rule_detail.get("validDataComparisonValues", [])
            rule_details_str = ",".join(str(v) for v in values)

        # -------- DEFAULT (unknown types) --------
        else:
            rule_details_str = json.dumps(rule_detail)

        # Write row
        writer.writerow([field_name, rule_type, rule_details_str])

print("✅ CSV created as per requirement!")




git checkout origin/main -- Pipfile Pipfile.lock ecbr_card_self_service/edq/common/scripts/edq_rule_engine.py

git commit -m "revert: remove unintended changes to Pipfile, Pipfile.lock and edq_rule_engine.py"


git push
