
import random

def generate_unique_data(num_ustaxid, num_pan):
    account_ids = set()
    values = set()
    ustaxid_data = []
    pan_data = []

    # Generate USTAXID records
    while len(ustaxid_data) < num_ustaxid:
        account_id = str(random.randint(10**16, 10**17 - 1))
        attribute = "Social Security Number"
        value = str(random.randint(10**8, 10**9 - 1))
        tokenization_type = "USTAXID"
        
        if account_id not in account_ids and value not in values:
            account_ids.add(account_id)
            values.add(value)
            ustaxid_data.append((account_id, attribute, value, tokenization_type))
    
    # Generate PAN records
    while len(pan_data) < num_pan:
        account_id = str(random.randint(10**16, 10**17 - 1))
        attribute = "Consumer Account Number"
        value = account_id
        tokenization_type = "PAN"
        
        if account_id not in account_ids and value not in values:
            account_ids.add(account_id)
            values.add(value)
            pan_data.append((account_id, attribute, value, tokenization_type))
    
    return ustaxid_data, pan_data

# Example usage
num_ustaxid = 10
num_pan = 10
ustaxid_data, pan_data = generate_unique_data(num_ustaxid, num_pan)

# Print the generated data to check
print("USTAXID Data:")
for record in ustaxid_data:
    print(record)

print("\nPAN Data:")
for record in pan_data:
    print(record)

# Create DataFrames
schema = ["account_id", "attribute", "value", "tokenization_type"]
ustaxid_df = spark.createDataFrame(ustaxid_data, schema)
pan_df = spark.createDataFrame(pan_data, schema)

ustaxid_df.show()
pan_df.show()
