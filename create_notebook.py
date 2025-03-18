import nbformat as nbf

# Create a new notebook
nb = nbf.v4.new_notebook()

# Title
title = nbf.v4.new_markdown_cell('# Email Associations Analysis\n\nThis notebook analyzes email associations in HubSpot tickets and compares ticket content with their first associations.')
nb.cells.append(title)

# Imports
imports = nbf.v4.new_code_cell('''import os
import pandas as pd
import requests
from datetime import datetime
import time
from tqdm import tqdm
import json
from dotenv import load_dotenv

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
}''')
nb.cells.append(imports)

# API Functions
api_functions = nbf.v4.new_code_cell('''def get_tickets(batch_size=100, after=None):
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
    return response.json()''')
nb.cells.append(api_functions)

# Fetch Tickets
fetch_tickets = nbf.v4.new_code_cell('''# Fetch all tickets
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

print(f"Total tickets fetched: {len(all_tickets)}")''')
nb.cells.append(fetch_tickets)

# Create DataFrame
create_df = nbf.v4.new_code_cell('''# Create DataFrame from tickets
tickets_df = pd.DataFrame(all_tickets)
tickets_df['id'] = tickets_df['id'].astype(str)
tickets_df.head()''')
nb.cells.append(create_df)

# Fetch Associations
fetch_associations = nbf.v4.new_code_cell('''# Fetch email associations for each ticket
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

# Create DataFrame from associations
associations_df = pd.DataFrame(ticket_associations)
associations_df.head()''')
nb.cells.append(fetch_associations)

# Merge Data
merge_data = nbf.v4.new_code_cell('''# Merge tickets and associations
merged_df = pd.merge(tickets_df, associations_df, left_on='id', right_on='ticket_id', how='left')
merged_df.head()''')
nb.cells.append(merge_data)

# Analyze Null Associations
analyze_null = nbf.v4.new_code_cell('''# Analysis of null association IDs
null_associations = merged_df[merged_df['association_id'].isna()]
print(f"Total tickets with null associations: {len(null_associations)}")
print(f"Percentage of tickets with null associations: {(len(null_associations) / len(tickets_df)) * 100:.2f}%")

# Group by ticket to see how many associations each ticket has
association_counts = merged_df.groupby('ticket_id')['association_id'].count().reset_index()
print("\\nAssociation count distribution:")
print(association_counts['association_id'].value_counts().sort_index())''')
nb.cells.append(analyze_null)

# Compare Function
compare_function = nbf.v4.new_code_cell('''# Function to compare ticket content with first association
def compare_ticket_with_first_association(ticket_id):
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
        return None''')
nb.cells.append(compare_function)

# Sample Analysis
sample_analysis = nbf.v4.new_code_cell('''# Compare a sample of tickets with their first associations
sample_size = 100
sample_tickets = tickets_df['id'].sample(n=sample_size)
comparison_results = []

for ticket_id in tqdm(sample_tickets):
    result = compare_ticket_with_first_association(ticket_id)
    if result:
        comparison_results.append(result)
    time.sleep(0.1)  # Rate limiting

# Create DataFrame from comparison results
comparison_df = pd.DataFrame(comparison_results)
comparison_df.head()''')
nb.cells.append(sample_analysis)

# Subject Matching Analysis
subject_analysis = nbf.v4.new_code_cell('''# Analyze subject matching
subject_matches = comparison_df['ticket_subject'] == comparison_df['email_subject']
match_percentage = (subject_matches.sum() / len(comparison_df)) * 100
print(f"Percentage of matching subjects: {match_percentage:.2f}%")

# Display examples of matches and mismatches
print("\\nExample matches:")
print(comparison_df[subject_matches][['ticket_subject', 'email_subject']].head())
print("\\nExample mismatches:")
print(comparison_df[~subject_matches][['ticket_subject', 'email_subject']].head())''')
nb.cells.append(subject_analysis)

# Write the notebook to a file
with open('email_associations.ipynb', 'w', encoding='utf-8') as f:
    nbf.write(nb, f) 