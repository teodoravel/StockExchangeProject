import requests
from bs4 import BeautifulSoup
import sqlite3
from pathlib import Path

# Функција за автоматско преземање на издавачите
def fetch_publisher_codes():
    url = 'https://www.mse.mk/mk/stats/symbolhistory/avk'  # url на Македонската берза за историски податоци
    response = requests.get(url)

    if response.status_code != 200:
        print("Не успеа да се преземе страницата")
        return []

    # Анализирање на html содржината
    soup = BeautifulSoup(response.text, 'html.parser')

    # Извлекување на сите издавачи од мени
    publisher_codes = []
    dropdown = soup.find('select', {'id': 'Code'})  # Наоѓаме селект елемент за издавачите
    if dropdown:
        options = dropdown.find_all('option')
        for option in options:
            code = option.get('value')  # Секој код на издавач
            # Филтрирање: Само кодови што содржат букви (без броеви и специјални знаци)
            if code and code.isalpha():  # Проверка дали кодот е само букви
                publisher_codes.append(code)

    return publisher_codes

# Функција за зачувување на издавачите во база на податоци
def save_to_database(publishers):
    # Дефинирање на апсолутната патека до базата 
    THIS_FOLDER = Path(__file__).parent.resolve()
    db_path = THIS_FOLDER / "publishers.db"

    # Поврзување со SQLite база на податоци (или креирање ако не постои)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Креирање на табела ако не постои
    cursor.execute('''CREATE TABLE IF NOT EXISTS publishers (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        publisher_code TEXT UNIQUE)''')

    # Проверка дали табелата постои пред да се обидеме да ја избришеме содржината
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='publishers';")
    if cursor.fetchone():
        # Бришење на постоечките податоци во табелата
        cursor.execute("DELETE FROM publishers")

    # Вметнување на издавачите во базата, со исклучување на дупликати
    for publisher_code in publishers:
        print(f"Вметнувам код на издавач: {publisher_code}")  # Прикажување на моменталниот издавач
        cursor.execute("INSERT OR IGNORE INTO publishers (publisher_code) VALUES (?)", (publisher_code,))

    # Чување на промените и затворање на врската
    conn.commit()
    conn.close()
    print(f"Успешно се вметнаа {len(publishers)} издавачи во базата на податоци.")

# main funct
def main():
    # Преземање на издавачите
    publisher_codes = fetch_publisher_codes()

    if publisher_codes:
        print(f"Пронајдени се {len(publisher_codes)} издавачи.")
        # Избегнување на дупликати и чување на единствени издавачи
        unique_codes = list(set(publisher_codes))
        save_to_database(unique_codes)
    else:
        print("Не беа пронајдени издавачи.")

# Извршување на програмата
if __name__ == '__main__':
    main()
