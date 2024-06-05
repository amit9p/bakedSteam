

if record_separator in ["TU", "transunion"]:
    record_separator = "^"
elif record_separator in ["EQ", "equifax"]:
    record_separator = "~"
elif record_separator in ["EX", "experian"]:
    record_separator = "\n"
else:
    record_separator = None  # Or handle default case appropriately
