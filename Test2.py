
import snowflake.connector
from snowflake.connector.errors import ProgrammingError

def connect_to_snowflake():
    return snowflake.connector.connect(
        user='<user>',
        password='<password>',
        account='<account>',
        warehouse='<warehouse>',
        database='<database>',
        schema='<schema>'
    )

def execute_query_with_retry(query, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            conn = connect_to_snowflake()
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            return results
        except ProgrammingError as e:
            # Check for token expiration error (typically error code 390114)
            if "authentication token expired" in str(e):
                print(f"Token expired. Attempting to reconnect... (Retry {retries+1}/{max_retries})")
                retries += 1
            else:
                raise e  # Raise any other exception
    raise Exception("Failed to refresh token after multiple retries.")

# Example usage
query = "SELECT CURRENT_TIMESTAMP()"
results = execute_query_with_retry(query)
print(results)
