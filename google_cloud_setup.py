import os
from google.oauth2 import service_account
from google.cloud import storage

def setup_google_cloud():
    # Path to your service account key JSON file
    credentials_path = "briqsafe-datawarehouse-dev-02c82a7f0ea7.json"
    
    # Create credentials object
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    
    # Create a storage client
    storage_client = storage.Client(credentials=credentials)
    
    # List all buckets (as a test)
    buckets = list(storage_client.list_buckets())
    print("Successfully connected to Google Cloud!")
    print(f"Found {len(buckets)} buckets:")
    for bucket in buckets:
        print(f"- {bucket.name}")
    
    return credentials

if __name__ == "__main__":
    setup_google_cloud() 