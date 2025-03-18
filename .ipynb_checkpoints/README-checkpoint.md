# HubSpot BigQuery Integration

This project provides tools for interacting with HubSpot data stored in Google BigQuery.

## Prerequisites

- Python 3.8 or higher
- Google Cloud credentials
- HubSpot API access

## Setup

### Windows

1. Create and activate virtual environment:
```powershell
python -m venv hubspot_bigquery
.\hubspot_bigquery\Scripts\activate
```

2. Install required packages:
```powershell
pip install -r requirements.txt
```

### macOS/Linux

1. Create and activate virtual environment:
```bash
python3 -m venv hubspot_bigquery
source hubspot_bigquery/bin/activate
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

## Configuration

1. Place your Google Cloud credentials JSON file in the project root directory
2. Update the credentials path in the scripts if needed

## Usage

### Fetching Tickets from BigQuery

Run the script to fetch tickets created since 2024:

```bash
python google_cloud_tickets_table_connection.py
```

The script will:
- Connect to BigQuery
- Fetch tickets created since January 1, 2024
- Display the data
- Save results to `tickets_2024.csv`

## Project Structure

- `google_cloud_tickets_table_connection.py`: Main script for fetching tickets from BigQuery
- `email_associations_extraction.py`: Script for extracting email associations
- `requirements.txt`: List of Python package dependencies

## Notes

- Make sure your virtual environment is activated before running scripts
- Keep your credentials secure and never commit them to version control
- The virtual environment directory (`hubspot_bigquery/`) is excluded from Git

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