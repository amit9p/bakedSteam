
import requests

# Step 1: Fetch the bearer token
token_url = 'https://api-precede.cloud.capitalone.com/oauth2/token'
token_data = {
    'client_id': 'some value',
    'client_secret': 'some value',
    'grant_type': 'client_credentials',
    'scope': 'tokenize:ustaxid detokenize:ustaxid'
}
token_headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
}

response = requests.post(token_url, data=token_data, headers=token_headers)

if response.status_code == 200:
    token = response.json().get('access_token')
    print(f"Token: {token}")
else:
    print(f"Failed to fetch token: {response.status_code}, {response.text}")
    exit()

# Step 2: Use the bearer token to call the second API
api_url = 'https://api-turing-precede.cloud.capitalone.com/enterprise/detokenize?type=USTAXID'
api_headers = {
    'Accept': 'application/json;v=3',
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}
api_data = {
    "values": [
        {
            "id": "Request 1",
            "value": "3EmFHFJgY"
        }
    ]
}

response = requests.post(api_url, json=api_data, headers=api_headers)

if response.status_code == 200:
    print("API Response:", response.json())
else:
    print(f"Failed to call API: {response.status_code}, {response.text}")
