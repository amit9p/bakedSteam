
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
