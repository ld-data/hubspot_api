from hubspot import HubSpot
from hubspot.crm.associations.models.batch_input_public_object_id import BatchInputPublicObjectId
import time
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Initialize HubSpot client
HUBSPOT_API_KEY = os.getenv('HUBSPOT_API_KEY')
hubspot_client = HubSpot(access_token=HUBSPOT_API_KEY)

def get_email_associations(ticket_id):
    """Get email associations for a ticket."""
    email_associations = []
    try:
        # Create a batch input with the ticket ID
        batch_ids = BatchInputPublicObjectId([{'id': str(ticket_id)}])
        
        # Get associations between ticket and emails
        print(f"Fetching email associations for ticket {ticket_id}...")
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
                        print(f"Fetching details for email {email_id}...")
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
    
    print(f"Found {len(email_associations)} email associations for ticket {ticket_id}")
    return email_associations

# Test with the specific ticket ID
ticket_id = "5140497394"
print(f"\nTesting with ticket ID: {ticket_id}")
results = get_email_associations(ticket_id)

# Print results
print("\nEmail Associations Results:")
for email in results:
    print(f"\nEmail ID: {email['association_id']}")
    print(f"Subject: {email['subject']}")
    print(f"Timestamp: {email['timestamp']}")
    print("-" * 50) 