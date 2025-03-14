from google.cloud import bigquery
import os
import pandas as pd
from datetime import datetime
from hubspot import HubSpot
import time
from tqdm import tqdm
from hubspot.crm.associations import BatchInputPublicObjectId

# Set up Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "briqsafe-datawarehouse-dev-02c82a7f0ea7.json"

# Initialize HubSpot client with your API key
HUBSPOT_API_KEY = os.getenv('HUBSPOT_API_KEY')
if not HUBSPOT_API_KEY:
    raise ValueError("Please set HUBSPOT_API_KEY environment variable")

hubspot_client = HubSpot(access_token=HUBSPOT_API_KEY)

def get_ticket_ids_from_bigquery():
    """Fetch ticket IDs and creation dates from BigQuery."""
    try:
        client = bigquery.Client()
        query = """
            SELECT 
                CAST(id as STRING) as ticket_id,
                property_createdate as creation_date,
                property_subject as ticket_subject,
                property_content as ticket_content
            FROM `briqsafe-datawarehouse-dev.hubspot_fivetran.ticket`
            WHERE DATE(property_createdate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        """
        query_job = client.query(query)
        rows = query_job.result()
        
        # Convert to DataFrame with proper column names
        df = pd.DataFrame(
            [dict(row.items()) for row in rows],
            columns=['ticket_id', 'creation_date', 'ticket_subject', 'ticket_content']
        )
        return df
    except Exception as e:
        print(f"Error fetching tickets from BigQuery: {e}")
        raise

def get_email_associations(ticket_id):
    """Get email associations for a ticket."""
    email_associations = []
    try:
        # Create a batch input with the ticket ID
        batch_ids = BatchInputPublicObjectId([{'id': str(ticket_id)}])
        
        # Get associations between ticket and emails
        associations = hubspot_client.crm.associations.batch_api.read(
            from_object_type="ticket",
            to_object_type="email",
            batch_input_public_object_id=batch_ids
        )
        
        if associations and associations.results:
            for result in associations.results:
                # Each result represents one ticket's associations
                for association in result.to:
                    try:
                        # Get email details using the association ID
                        email_id = association.id
                        email_details = hubspot_client.crm.objects.basic_api.get_by_id(
                            object_type="email",
                            object_id=email_id,
                            properties=["hs_email_subject", "hs_email_text", "hs_timestamp"]
                        )
                        
                        email_associations.append({
                            'association_id': email_id,
                            'subject': email_details.properties.get('hs_email_subject', ''),
                            'content': email_details.properties.get('hs_email_text', ''),
                            'timestamp': email_details.properties.get('hs_timestamp', '')
                        })
                        time.sleep(0.1)  # Rate limiting
                    except Exception as e:
                        print(f"Error fetching email details for ticket {ticket_id}, email {email_id}: {str(e)}")
                        continue
    except Exception as e:
        print(f"Error fetching associations for ticket {ticket_id}: {str(e)}")
    
    return email_associations

def main():
    # Get ticket IDs from BigQuery
    print("Fetching tickets from BigQuery...")
    tickets_df = get_ticket_ids_from_bigquery()
    
    # Debug: print DataFrame info
    print("\nDataFrame Info:")
    print(tickets_df.info())
    print("\nFirst few rows:")
    print(tickets_df.head())
    
    # Prepare the results list
    results = []
    
    # Process each ticket
    print("\nProcessing tickets and their email associations...")
    for index, row in tqdm(tickets_df.iterrows(), total=len(tickets_df)):
        # Add the original ticket as a row with null association_id
        results.append({
            'ticket_id': str(row['ticket_id']),  # Using correct column name
            'association_id': None,
            'subject': row['ticket_subject'],  # Using correct column names
            'content': row['ticket_content'],
            'timestamp': row['creation_date']
        })
        
        # Get and add email associations
        email_associations = get_email_associations(str(row['ticket_id']))  # Using correct column name
        for email in email_associations:
            results.append({
                'ticket_id': str(row['ticket_id']),
                'association_id': email['association_id'],
                'subject': email['subject'],
                'content': email['content'],
                'timestamp': email['timestamp']
            })
    
    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
    
    # Save to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"ticket_email_associations_{timestamp}.csv"
    results_df.to_csv(output_file, index=False)
    print(f"\nResults saved to {output_file}")
    
    # Print summary statistics
    print(f"\nSummary:")
    print(f"Total tickets processed: {len(tickets_df)}")
    print(f"Total rows (including associations): {len(results_df)}")
    print(f"Tickets with email associations: {len(results_df[results_df['association_id'].notna()])}")

if __name__ == "__main__":
    main() 