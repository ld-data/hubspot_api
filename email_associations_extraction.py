#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This script extracts email associations from HubSpot ticket data in BigQuery.
It connects to BigQuery, queries the relevant tables, and processes email-related data.
"""

import os
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EmailAssociationsExtractor:
    def __init__(self, credentials_path='credentials.json'):
        """
        Initialize the EmailAssociationsExtractor with Google Cloud credentials.
        
        Args:
            credentials_path (str): Path to the Google Cloud credentials JSON file
        """
        try:
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            self.client = bigquery.Client(credentials=self.credentials)
            logger.info("Successfully initialized BigQuery client")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {str(e)}")
            raise

    def extract_email_associations(self, project_id="briqsafe-datawarehouse-dev", 
                                 dataset_id="hubspot_fivetran",
                                 table_id="ticket"):
        """
        Extract email associations from HubSpot ticket data.
        
        Args:
            project_id (str): Google Cloud project ID
            dataset_id (str): BigQuery dataset ID
            table_id (str): BigQuery table ID
        
        Returns:
            pandas.DataFrame: DataFrame containing email associations
        """
        try:
            query = f"""
                SELECT 
                    t.id as ticket_id,
                    t.subject,
                    t.content as ticket_content,
                    t.created_at,
                    t.updated_at,
                    t.owner_id,
                    t.pipeline,
                    t.pipeline_stage,
                    t.associated_company_ids,
                    t.associated_contact_ids,
                    t.associated_deal_ids
                FROM `{project_id}.{dataset_id}.{table_id}` t
                WHERE t.content IS NOT NULL
                AND t.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
                ORDER BY t.created_at DESC
                LIMIT 1000
            """
            
            logger.info("Executing BigQuery query...")
            df = self.client.query(query).to_dataframe()
            logger.info(f"Retrieved {len(df)} records from BigQuery")
            
            return df
            
        except Exception as e:
            logger.error(f"Error executing BigQuery query: {str(e)}")
            raise

    def process_email_data(self, df):
        """
        Process the extracted email data.
        
        Args:
            df (pandas.DataFrame): DataFrame containing raw email data
        
        Returns:
            pandas.DataFrame: Processed email associations data
        """
        try:
            # Add processing logic here
            # For example:
            # - Extract email addresses from content
            # - Analyze email threads
            # - Map associations between contacts
            
            logger.info("Processing email data...")
            
            # Example processing (you can modify this based on your needs)
            processed_df = df.copy()
            processed_df['processed_at'] = datetime.now()
            
            return processed_df
            
        except Exception as e:
            logger.error(f"Error processing email data: {str(e)}")
            raise

    def save_results(self, df, output_path='email_associations.csv'):
        """
        Save the processed results to a file.
        
        Args:
            df (pandas.DataFrame): Processed data to save
            output_path (str): Path where to save the results
        """
        try:
            df.to_csv(output_path, index=False)
            logger.info(f"Results saved to {output_path}")
        except Exception as e:
            logger.error(f"Error saving results: {str(e)}")
            raise

def main():
    """Main function to run the email associations extraction process."""
    try:
        # Initialize the extractor
        extractor = EmailAssociationsExtractor()
        
        # Extract data
        raw_data = extractor.extract_email_associations()
        
        # Process data
        processed_data = extractor.process_email_data(raw_data)
        
        # Save results
        extractor.save_results(processed_data)
        
        logger.info("Email associations extraction completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main() 