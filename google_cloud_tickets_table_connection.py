from google.cloud import bigquery
import os
import pandas as pd
from datetime import datetime, timedelta

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
    
    # Print all column names
    print("\nAvailable columns in the table:")
    for field in table.schema:
        print(f"- {field.name} ({field.field_type})")

    # Calculate date one month ago
    one_month_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    # Query to fetch all tickets created in the last month
    query = f"""
        SELECT *
        FROM `{table_id}`
        WHERE DATE(property_createdate) >= '{one_month_ago}'
        ORDER BY property_createdate DESC
    """

    print(f"\nExecuting query to fetch tickets created since {one_month_ago}...")
    
    # Execute the query and convert to DataFrame
    df = client.query(query).to_dataframe()
    
    # Print summary statistics
    print(f"\nFound {len(df)} tickets created in the last month")
    print(f"Number of columns: {len(df.columns)}")
    
    # Save the results to a CSV file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"tickets_last_month_{timestamp}.csv"
    df.to_csv(output_file, index=False)
    print(f"\nResults saved to {output_file}")
    
    # Display the first few rows with key columns
    print("\nFirst few tickets (showing key columns):")
    key_columns = ['id', 'property_subject', 'property_content', 'property_createdate', 'property_hs_lastmodifieddate', 'property_hubspot_owner_id', 'property_hs_pipeline', 'property_hs_pipeline_stage']
    print(df[key_columns].head().to_string())

except Exception as e:
    print(f"Error accessing BigQuery: {e}")

