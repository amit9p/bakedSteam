
import requests
import random
import json

# Generate 1000 IDs with random 9-digit values
api_data = {
    "values": [{"id": str(i), "value": str(random.randint(100000000, 999999999))} for i in range(1, 1001)]
}

# Print a sample to verify
print(api_data)

# Set up the API endpoint and headers
api_url = "https://api-turing-precede.cloud.capitalone.com/enterprise/tokenize?type=USTAXID"
api_headers = {
    "Accept": "application/json;v=3",
    "Authorization": "Bearer {token}",  # Ensure {token} is replaced with the actual token variable
    "Content-Type": "application/json"
}

# Make the API call
response = requests.post(api_url, json=api_data, headers=api_headers)

# Check the response status and process the response
if response.status_code == 200:
    response_data = response.json()

    # Initialize the dictionary to store the ID and returned value
    result_dict = {}

    # Extract the values from the response and store them in the dictionary
    for item in response_data['values']:
        result_dict[item['id']] = item['value']

    # Print the result dictionary to verify
    print(result_dict)
else:
    print(f"Failed to fetch data. Status code: {response.status_code}, Response: {response.text}")
