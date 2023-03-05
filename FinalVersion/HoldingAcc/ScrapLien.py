import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
from tqdm import tqdm
import csv

# Read the existing links from the file
if os.path.isfile('liens.csv'):
    df = pd.read_csv('liens.csv')
    existing_links = set()
else:
    existing_links = set()

new_links = []
temp = 0


# Check how many pages from file to determine where to start scraping
# range is now (first_page, last_page)

# first_page = lengh of liens.csv
with open('liens.csv') as csvfile:
    reader = csv.reader(csvfile)
    row_count = len(list(reader))
# print("Number of rows:", row_count)

first_page = row_count // 20
print("First page: {}".format(first_page))

url1 = 'https://ec.europa.eu/clima/ets/oha.do?form=oha&languageCode=fr&accountHolder=&installationIdentifier=&installationName=&permitIdentifier=&mainActivityType=-1&search=Search&searchType=oha&currentSortSettings='
response = requests.get(url1)
soup = BeautifulSoup(response.content, 'lxml')
last_page = int(soup.find('input', {'name': 'resultList.lastPageNumber'}).get('value'))
print("Last page: {}".format(last_page))


# Scrape new links
for i in tqdm(range(first_page, last_page)):
    url = "https://ec.europa.eu/clima/ets/oha.do?form=oha&languageCode=fr&accountHolder=&installationIdentifier=&installationName=&permitIdentifier=&mainActivityType=-1&searchType=oha&currentSortSettings=&resultList.currentPageNumber=" + str(i) + "&nextList=Next>"


    while True:
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'lxml')
            tables = soup.find_all('table', {'id': 'tblAccountSearchResult'})
            break
        except:
            print("Connection error, retrying in 2 seconds...")
            temp += 1
            time.sleep(2)




    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            count = 0
            cells = row.find_all('td')
            for cell in cells:
                for a in cell.find_all('a', class_='listlink', href=True):
                    count += 1
                    if count == 2:
                        link = a['href']
                        if link not in existing_links:
                            new_links.append(link)
                            existing_links.add(link)
                        break
                if count == 2:
                    break


# Update the CSV file with the existing links
df = pd.DataFrame(list(existing_links), columns=['Link'])
df.to_csv('liens.csv', index=False, header=False)
