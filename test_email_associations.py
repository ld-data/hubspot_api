from hubspot import HubSpot
import time
from hubspot.crm.associations.models.batch_input_public_object_id import BatchInputPublicObjectId
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Initialize HubSpot client with your API key
HUBSPOT_API_KEY = os.getenv('HUBSPOT_API_KEY')
if not HUBSPOT_API_KEY:
    raise ValueError("Please set HUBSPOT_API_KEY environment variable in .env file")

print("Initializing HubSpot client...")
hubspot_client = HubSpot(access_token=HUBSPOT_API_KEY)

def get_email_associations(ticket_id):
    """Get email associations for a ticket."""
    email_associations = []
    try:
        print(f"Creating batch input for ticket ID: {ticket_id}")
        # Create a batch input with the ticket ID
        batch_ids = BatchInputPublicObjectId([{'id': str(ticket_id)}])
        
        # Get associations between ticket and emails
        print(f"Fetching email associations for ticket {ticket_id}...")
        try:
            associations = hubspot_client.crm.associations.batch_api.read(
                from_object_type="ticket",
                to_object_type="email",
                batch_input_public_object_id=batch_ids
            )
            print(f"Successfully got associations response: {associations}")
        except Exception as api_error:
            print(f"Error in HubSpot API call: {str(api_error)}")
            print(f"API Key being used: {HUBSPOT_API_KEY[:5]}...")  # Only show first 5 chars for security
            raise
        
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
        else:
            print("No associations found in the response")
    except Exception as e:
        print(f"Error fetching associations for ticket {ticket_id}: {str(e)}")
        print(f"Full error details: {type(e).__name__}: {str(e)}")
    
    print(f"Found {len(email_associations)} email associations for ticket {ticket_id}")
    return email_associations

def main():
    try:
        print("Starting script...")
        print(f"HubSpot API Key loaded: {'Yes' if HUBSPOT_API_KEY else 'No'}")
        
        # Test with a specific ticket ID
        test_ticket_id = "87009131767"  # This is a ticket ID from our previous successful run
        print(f"\nTesting get_email_associations with ticket ID: {test_ticket_id}")
        email_associations = get_email_associations(test_ticket_id)
        
        # Print the results
        print("\nEmail Associations Results:")
        for email in email_associations:
            print(f"\nEmail ID: {email['association_id']}")
            print(f"Subject: {email['subject']}")
            print(f"Timestamp: {email['timestamp']}")
            print("-" * 50)
        
    except Exception as e:
        print(f"An error occurred in the main process: {str(e)}")
        print(f"Full error details: {type(e).__name__}: {str(e)}")
        raise

if __name__ == "__main__":
    main() 