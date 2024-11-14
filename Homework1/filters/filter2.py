import requests
import sqlite3
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import json
from pathlib import Path

# Define paths for the database and JSON file to store last dates
THIS_FOLDER = Path(__file__).parent.resolve()
DB_PATH = THIS_FOLDER / "stock_data.db"
LAST_DATES_PATH = THIS_FOLDER / "last_dates.json"

# MSE URL for historical data
BASE_URL = 'https://www.mse.mk/mk/stats/symbolhistory/'

# Function to get the last available date for a publisher
def get_last_data_date(publisher_code):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Check for the most recent date
    cursor.execute("SELECT MAX(date) FROM stock_data WHERE publisher_code = ?", (publisher_code,))
    last_date = cursor.fetchone()[0]
    conn.close()

    return last_date if last_date else None

# Function to fetch stock data from MSE
def fetch_stock_data(publisher_code, from_date, to_date):
    params = {
        'FromDate': from_date,
        'ToDate': to_date,
        'Code': publisher_code
    }
    response = requests.get(BASE_URL + publisher_code, params=params)

    if response.status_code == 200:
        return response.text
    else:
        print(f"Error fetching data for {publisher_code}. Status code: {response.status_code}")
        return None

# Parse stock data table from HTML
def parse_stock_table(html):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', {'id': 'resultsTable'})
    if not table:
        print("Table not found!")
        return []

    rows = table.find_all('tr')
    data = []

    for row in rows[1:]:  # Skip header row
        cols = row.find_all('td')
        if len(cols) > 1:
            data.append({
                'Date': cols[0].text.strip(),
                'Price': cols[1].text.strip(),
                'Max': cols[2].text.strip(),
                'Min': cols[3].text.strip(),
                'Avg': cols[4].text.strip(),
                'Percent Change': cols[5].text.strip(),
                'Quantity': cols[6].text.strip(),
                'Best Turnover': cols[7].text.strip(),
                'Total Turnover': cols[8].text.strip()
            })
    return data

# Function to save stock data in the database
def save_to_database(publisher_code, data):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for record in data:
        cursor.execute('''INSERT OR REPLACE INTO stock_data (publisher_code, date, price, max, min, avg, percent_change, quantity, best_turnover, total_turnover)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
            publisher_code,
            record['Date'],
            record['Price'],
            record['Max'],
            record['Min'],
            record['Avg'],
            record['Percent Change'],
            record['Quantity'],
            record['Best Turnover'],
            record['Total Turnover']
        ))

    conn.commit()
    conn.close()
    print(f"Data saved for {publisher_code} in the database.")

# Process all publishers and save last dates to JSON
def process_publishers(publisher_codes):
    last_dates = {}

    for publisher_code in publisher_codes:
        last_date = get_last_data_date(publisher_code)

        if not last_date:
            print(f"Issuer {publisher_code} has no data. Fetching data for the last 10 years.")
            from_date = '01.01.2014'
            to_date = datetime.now().strftime('%d.%m.%Y')
            html = fetch_stock_data(publisher_code, from_date, to_date)
            if html:
                data = parse_stock_table(html)
                if data:
                    save_to_database(publisher_code, data)
                    # Record the latest date we fetched, which is the end date of the request
                    last_dates[publisher_code] = to_date
        else:
            print(f"Issuer {publisher_code} has data up to {last_date}.")
            last_dates[publisher_code] = last_date

    # Save the last dates to JSON
    with open(LAST_DATES_PATH, 'w') as json_file:
        json.dump(last_dates, json_file)
    print("Last dates saved to last_dates.json")

# Fetch publisher codes from the webpage
def fetch_publisher_codes():
    url = 'https://www.mse.mk/mk/stats/symbolhistory/avk'
    response = requests.get(url)

    if response.status_code != 200:
        print("Failed to retrieve the page")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    publisher_codes = []
    dropdown = soup.find('select', {'id': 'Code'})
    if dropdown:
        options = dropdown.find_all('option')
        for option in options:
            code = option.get('value')
            if code and code.isalpha():  # Only include codes with letters
                publisher_codes.append(code)

    return publisher_codes

# Main function
def main():
    publisher_codes = fetch_publisher_codes()
    if publisher_codes:
        process_publishers(publisher_codes)
    else:
        print("No publishers found.")

if __name__ == '__main__':
    main()
