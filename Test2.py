
CAST(TO_DATE(NULLIF(AUTO_LATE_FEE_WAIVER_REQUEST_V2.FEE_WAIVER_EFFECTIVE_DATE, ''), 'YYYY-MM-DD') AS DATE) AS FEE_WAIVR_EFF_DT



credentials.json

{
  "installed": {
    "client_id": "YOUR_CLIENT_ID.apps.googleusercontent.com",
    "project_id": "YOUR_PROJECT_ID",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_secret": "YOUR_CLIENT_SECRET",
    "redirect_uris": [
      "urn:ietf:wg:oauth:2.0:oob",
      "http://localhost"
    ]
  }
}





import gspread
from google.oauth2.service_account import Credentials

# Path to your service account key file
creds = Credentials.from_service_account_file('path/to/credentials.json')
scoped_creds = creds.with_scopes(['https://www.googleapis.com/auth/spreadsheets'])

# Initialize Google Sheets client
client = gspread.authorize(scoped_creds)



import snowflake.connector

# Snowflake connection details
connection_params = {
    'user': 'your_username',
    'password': 'your_password',
    'account': 'your_account',
    'warehouse': 'your_warehouse',
    'database': 'your_database',
    'schema': 'your_schema'
}

# Connect to Snowflake
conn = snowflake.connector.connect(**connection_params)
cursor = conn.cursor()

# Query to fetch data from Snowflake
query = "SELECT * FROM your_table LIMIT 10"
cursor.execute(query)
rows = cursor.fetchall()
cursor.close()
conn.close()



# Google Sheet ID and sheet name
SPREADSHEET_ID = 'your_spreadsheet_id'
SHEET_NAME = 'Sheet1'  # Change to your sheet name

# Open the Google Sheet
sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

# Clear the existing content
sheet.clear()

# Prepare data for Google Sheets
data = [list(map(str, row)) for row in rows]  # Convert rows to string for Google Sheets

# Add headers if needed (assuming you want column names as headers)
header = ['Column1', 'Column2', 'Column3']  # Replace with your actual column names
sheet.append_row(header)

# Insert data into Google Sheet
for row in data:
    sheet.append_row(row)
