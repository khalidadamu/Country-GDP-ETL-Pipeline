import requests
from bs4 import BeautifulSoup
import json
import sqlite3
import logging

# Setup logging
logging.basicConfig(filename='etl_project_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_gdp_data(url):
    logging.info("Starting extraction of GDP data")
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'class': 'wikitable'})
        return table
    else:
        logging.error(f"Failed to retrieve the webpage, status code: {response.status_code}")
        return None

def transform_gdp_data(table):
    logging.info("Starting transformation of GDP data")
    countries_gdp = []
    rows = table.find_all('tr')
    for row in rows[1:]:
        cols = row.find_all('td')
        if len(cols) >= 2:
            country = cols[1].text.strip()
            gdp = cols[2].text.strip().replace(',', '')
            try:
                gdp = float(gdp)
                countries_gdp.append({"Country": country, "GDP_USD_billion": round(gdp, 2)})
            except ValueError:
                logging.warning(f"Skipping row due to conversion error: {cols}")
                continue
    return countries_gdp

def load_to_json(countries_gdp, json_file):
    logging.info(f"Loading data into JSON file: {json_file}")
    with open(json_file, 'w') as file:
        json.dump(countries_gdp, file, indent=4)

def load_to_sqlite(countries_gdp, db_file):
    logging.info(f"Loading data into SQLite database: {db_file}")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS Countries_by_GDP
                      (Country TEXT, GDP_USD_billion REAL)''')
    cursor.execute('DELETE FROM Countries_by_GDP')
    for entry in countries_gdp:
        cursor.execute('''INSERT INTO Countries_by_GDP (Country, GDP_USD_billion)
                          VALUES (?, ?)''', (entry['Country'], entry['GDP_USD_billion']))
    conn.commit()
    conn.close()

def query_database(db_file):
    logging.info(f"Querying database: {db_file} for entries with GDP > 100 billion USD")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute('''SELECT * FROM Countries_by_GDP WHERE GDP_USD_billion <= 100''')
    result = cursor.fetchall()
    conn.close()
    return result

def main():
    url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
    json_file = 'Countries_by_GDP.json'
    db_file = 'World_Economies.db'

    # ETL process
    table = extract_gdp_data(url)
    if table:
        countries_gdp = transform_gdp_data(table)
        load_to_json(countries_gdp, json_file)
        load_to_sqlite(countries_gdp, db_file)
        
        # Query the database
        result = query_database(db_file)
        logging.info(f"Query Result: {result}")
        print("Entries with GDP > 100 billion USD:")
        for row in result:
            print(row)
    else:
        logging.error("ETL process failed due to extraction error")

if __name__ == '__main__':
    main()
