@echo off
call hubspot_google_env\Scripts\activate.bat
python google_cloud_tickets_table_connection.py
pause 