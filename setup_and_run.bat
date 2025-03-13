@echo off
echo Activating virtual environment...
call hubspot_google_env\Scripts\activate.bat

echo Installing required packages...
python -m pip install --upgrade pip
pip install google-cloud-bigquery google-cloud-storage google-auth

echo Running the script...
python google_cloud_tickets_table_connection.py
pause 