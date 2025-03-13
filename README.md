# HubSpot BigQuery Integration

This project provides functionality to connect to and query HubSpot ticket data from Google BigQuery.

## Prerequisites

- Python 3.8 or higher
- Google Cloud Platform account with BigQuery access
- HubSpot account with API access
- Required Python packages (listed in requirements.txt)

## Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd hubspot-api
```

2. Create and activate a virtual environment:
```bash
# Windows
python -m venv hubspot_bigquery
.\hubspot_bigquery\Scripts\activate

# Unix/MacOS
python3 -m venv hubspot_bigquery
source hubspot_bigquery/bin/activate
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

4. Set up Google Cloud credentials:
   - Place your Google Cloud service account credentials JSON file in the project directory
   - Ensure the credentials file is named `credentials.json`

## Usage

1. Activate the virtual environment if not already activated:
```bash
# Windows
.\hubspot_bigquery\Scripts\activate

# Unix/MacOS
source hubspot_bigquery/bin/activate
```

2. Run the BigQuery connection script:
```bash
python google_cloud_tickets_table_connection.py
```

## Project Structure

- `google_cloud_tickets_table_connection.py`: Main script for connecting to BigQuery and querying HubSpot ticket data
- `requirements.txt`: List of Python package dependencies
- `credentials.json`: Google Cloud service account credentials (not included in repository)

## Security Notes

- Never commit sensitive credentials or API keys to the repository
- Keep your credentials.json file secure and local to your development environment
- Use environment variables for sensitive configuration when possible

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Add your license information here] 