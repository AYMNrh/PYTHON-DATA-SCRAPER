import requests
from bs4 import BeautifulSoup
import time
import pandas as pd

with open('links.txt', 'r') as f:
    urls = f.readlines()

with open('Jega/tableauww.csv', 'w', encoding='utf-8') as out_file:
    for url in urls:
        url = url.strip()  # remove whitespace characters (e.g. \n)

        while True:
            try:
                response = requests.get(url)
                soup = BeautifulSoup(response.content, 'lxml')
                tables = soup.find_all('table', {'id': 'tblChildDetails'})
                break
            except:
                print("Connection error, retrying in 2 seconds...")
                time.sleep(2)

        data = []
        for table in tables:
            cells = table.find_all('td', {'class': 'bgcelllist'})
            for cell in cells[:-1]:
                span = cell.find('span', {'class': 'classictext'})
                data.append(span.text.strip())

        # Remove last 7 elements from data
        data = data[:-14]

        # Convert data to DataFrame and write to CSV file
        df = pd.DataFrame([data])
        df.to_csv(out_file, header=False, index=False, line_terminator='\n')

