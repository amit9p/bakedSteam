
import random

def generate_unique_data(num_records, num_ustaxid, num_pan):
    account_ids = set()
    values = set()
    data = []

    # Generate USTAXID records
    while len([record for record in data if record[3] == 'USTAXID']) < num_ustaxid:
        account_id = str(random.randint(10**16, 10**17 - 1))
        attribute = "Social Security Number"
        value = str(random.randint(10**8, 10**9 - 1))
        tokenization_type = "USTAXID"
        
        if account_id not in account_ids and value not in values:
            account_ids.add(account_id)
            values.add(value)
            data.append((account_id, attribute, value, tokenization_type))
    
    # Generate PAN records
    while len([record for record in data if record[3] == 'PAN']) < num_pan:
        account_id = str(random.randint(10**16, 10**17 - 1))
        attribute = "Consumer Account Number"
        value = account_id
        tokenization_type = "PAN"
        
        if account_id not in account_ids and value not in values:
            account_ids.add(account_id)
            values.add(value)
            data.append((account_id, attribute, value, tokenization_type))
    
    return data

# Example usage
num_ustaxid = 10
num_pan = 10
data = generate_unique_data(20, num_ustaxid, num_pan)

# Print the generated data to check
for record in data:
    print(record)

# Create DataFrame
schema = ["account_id", "attribute", "value", "tokenization_type"]
df = spark.createDataFrame(data, schema)
df.show()
