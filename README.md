# ETL Project: Extracting GDP Data from IMF


This project involves creating an automated script to extract, transform, and load (ETL) GDP data of all countries as logged by the International Monetary Fund (IMF). The script will be used by an international firm looking to expand its business across different countries. The data will be updated twice a year based on IMF's releases.

# Objectives
Extract: Retrieve the list of all countries and their GDPs in billion USD from the IMF webpage.
Transform: Process the data to round the GDP values to two decimal places.
Load: Save the processed data to a JSON file and a SQLite database table.
Query: Demonstrate the successful execution of the ETL process by querying the database for entries with a GDP greater than 100 billion USD.
Log: Record the entire process in a log file.

