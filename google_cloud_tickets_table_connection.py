from google.cloud import bigquery
import os

# Set the path to your service account key JSON file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "briqsafe-datawarehouse-dev-02c82a7f0ea7.json"

# Initialize the BigQuery client
try:
    client = bigquery.Client()
    print("Successfully authenticated with BigQuery")
except Exception as e:
    print(f"Authentication failed: {e}")
    raise

# Define the table ID
table_id = "briqsafe-datawarehouse-dev.hubspot_fivetran.ticket"

# Create a reference to the table
table_ref = client.dataset("hubspot_fivetran", project="briqsafe-datawarehouse-dev").table("ticket")

try:
    # Test the connection by getting table metadata
    table = client.get_table(table_ref)
    print(f"Successfully connected to table {table_id}")
    print(f"Table has {table.num_rows} rows and {len(table.schema)} columns")

    # Example query to fetch data from the table
    query = f"""
        SELECT *
        FROM `{table_id}`
        LIMIT 10
    """

    # Execute the query
    query_job = client.query(query)
    results = query_job.result()

    # Print the results
    print("\nFirst 10 rows from the table:")
    for row in results:
        print(row)

except Exception as e:
    print(f"Error accessing BigQuery: {e}")

