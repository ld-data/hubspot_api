import os
import pandas as pd
import requests
from datetime import datetime
import time
from tqdm import tqdm
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HubSpot Email Associations") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.2.0") \
    .getOrCreate()

# Load environment variables
load_dotenv()

# Get HubSpot API key
HUBSPOT_API_KEY = os.getenv('HUBSPOT_API_KEY')
if not HUBSPOT_API_KEY:
    raise ValueError("HUBSPOT_API_KEY not found in environment variables")

# API endpoints
BASE_URL = "https://api.hubapi.com"
TICKETS_ENDPOINT = f"{BASE_URL}/crm/v3/objects/tickets"
ASSOCIATIONS_ENDPOINT = f"{BASE_URL}/crm/v3/objects/tickets/associations/emails"
EMAILS_ENDPOINT = f"{BASE_URL}/crm/v3/objects/emails"

# Headers for API requests
headers = {
    "Authorization": f"Bearer {HUBSPOT_API_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

def get_tickets(batch_size=100, after=None):
    """Fetch tickets from HubSpot with pagination"""
    params = {
        "limit": batch_size,
        "properties": ["hs_ticket_id", "subject", "hs_ticket_priority", "createdate", "hs_pipeline", "hs_pipeline_stage"],
        "archived": False
    }
    
    if after:
        params["after"] = after
    
    response = requests.get(TICKETS_ENDPOINT, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def get_email_associations(ticket_id):
    """Fetch email associations for a specific ticket"""
    params = {
        "limit": 100,
        "archived": False
    }
    
    url = f"{ASSOCIATIONS_ENDPOINT}/{ticket_id}/to/email"
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def get_email_details(email_id):
    """Fetch details of a specific email"""
    url = f"{EMAILS_ENDPOINT}/{email_id}"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def fetch_all_tickets():
    """Fetch all tickets from HubSpot"""
    all_tickets = []
    after = None
    
    while True:
        try:
            response = get_tickets(after=after)
            tickets = response.get('results', [])
            all_tickets.extend(tickets)
            
            # Check if there are more pages
            paging = response.get('paging', {})
            next_page = paging.get('next', {}).get('after')
            
            if not next_page:
                break
                
            after = next_page
            time.sleep(0.1)  # Rate limiting
            
        except Exception as e:
            print(f"Error fetching tickets: {e}")
            break
    
    return all_tickets

def fetch_associations(tickets_df):
    """Fetch email associations for all tickets"""
    ticket_associations = []
    
    for ticket_id in tqdm(tickets_df['id']):
        try:
            associations = get_email_associations(ticket_id)
            results = associations.get('results', [])
            
            for assoc in results:
                ticket_associations.append({
                    'ticket_id': ticket_id,
                    'association_id': assoc.get('id'),
                    'association_type': assoc.get('associationTypeId'),
                    'association_category': assoc.get('associationCategory')
                })
            
            time.sleep(0.1)  # Rate limiting
            
        except Exception as e:
            print(f"Error fetching associations for ticket {ticket_id}: {e}")
            continue
    
    return ticket_associations

def compare_ticket_with_first_association(ticket_id, tickets_df):
    """Compare ticket content with its first association"""
    try:
        # Get ticket details
        ticket = tickets_df[tickets_df['id'] == ticket_id].iloc[0]
        
        # Get first association
        associations = get_email_associations(ticket_id)
        if not associations.get('results'):
            return None
            
        first_assoc = associations['results'][0]
        email_id = first_assoc.get('id')
        
        # Get email details
        email = get_email_details(email_id)
        
        return {
            'ticket_id': ticket_id,
            'ticket_subject': ticket.get('properties', {}).get('subject'),
            'email_subject': email.get('properties', {}).get('hs_email_subject'),
            'email_text': email.get('properties', {}).get('hs_email_text'),
            'email_html': email.get('properties', {}).get('hs_email_html')
        }
    except Exception as e:
        print(f"Error comparing ticket {ticket_id}: {e}")
        return None

def main():
    # Create timestamp for file naming
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Fetch all tickets
    print("Fetching tickets...")
    all_tickets = fetch_all_tickets()
    tickets_df = pd.DataFrame(all_tickets)
    tickets_df['id'] = tickets_df['id'].astype(str)
    
    # Fetch associations
    print("Fetching associations...")
    ticket_associations = fetch_associations(tickets_df)
    associations_df = pd.DataFrame(ticket_associations)
    
    # Merge data
    merged_df = pd.merge(tickets_df, associations_df, left_on='id', right_on='ticket_id', how='left')
    
    # Save merged data to ADLS Gen2
    storage_account = "azureadls2centralsweden"
    container = "raw"
    folder = "hubspot"
    
    # Define the mount point
    mount_point = f"/mnt/{storage_account}/{container}"
    
    # Create mount point if it doesn't exist
    if not os.path.exists(mount_point):
        dbutils.fs.mount(
            source=f"abfss://{container}@{storage_account}.blob.core.windows.net/",
            mount_point=mount_point,
            extra_configs={
                f"fs.azure.account.key.{storage_account}.blob.core.windows.net": dbutils.secrets.get(scope="hubspot", key="storage-account-key")
            }
        )
    
    # Save merged data to ADLS Gen2
    merged_df.to_csv(f"{mount_point}/{folder}/merged_data_{timestamp}.csv", index=False)
    
    print(f"Data has been successfully saved to ADLS Gen2: {mount_point}/{folder}/merged_data_{timestamp}.csv")

if __name__ == "__main__":
    main() 

"""
-- BigQuery SQL Query for HubSpot Ticket Analysis
SELECT 
    CAST(ticket_id as STRING) as ticket_id,
    create_date as creation_date,
    subject as ticket_subject,
    r.property_content as ticket_content
FROM `briqsafe-datawarehouse.marts.mart_hubspot_tickets` t  
left join `briqsafe-datawarehouse-dev.hubspot_fivetran.ticket` r on r.id = t.ticket_id
WHERE DATE(create_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
AND pipeline_name = 'Support' AND source_type = 'EMAIL'
""" 