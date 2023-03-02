import csv
import datetime
import time
import requests
from bs4 import BeautifulSoup
import os


# scrap la dernier page de Holding Account




# define the header row for the CSV file
header = ["National Administrator", "Account Type", "Account Holder Name", "Installation/Aircraft ID", "Installation Name/Aircraft Operator Code", "Company Registration No", "Permit/Plan ID", "Permit/Plan Date", "Main Activity Type", "Latest Compliance Code"]

# Create an empty list to store the scraped data
data = []

# Read the existing transaction IDs from the previously scraped CSV file
existing_ids = set()
try:
    with open('Jega/HoldingAcc.csv', 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # skip header row
        for row in reader:
            if len(row) == len(header):
                existing_ids.add(row[3])  # ID is in the fourth column
except FileNotFoundError:
    print("No existing transaction IDs found")

# A variable that is used to count the number of times the connection was lost.
temp = 0;



# Check how many pages from file to determine where to start scraping
# range is now (first_page, last_page)
url1 = 'https://ec.europa.eu/clima/ets/oha.do?form=oha&languageCode=fr&accountHolder=&installationIdentifier=&installationName=&permitIdentifier=&mainActivityType=-1&search=Search&searchType=oha&currentSortSettings='
response = requests.get(url1)
soup = BeautifulSoup(response.content, 'lxml')
last_page = int(soup.find('input', {'name': 'resultList.lastPageNumber'}).get('value'))
print("Last page: {}".format(last_page))
last_new_page =  last_page + 1




for i in range(0, 3):
    url = "https://ec.europa.eu/clima/ets/oha.do?form=oha&languageCode=fr&accountHolder=&installationIdentifier=&installationName=&permitIdentifier=&mainActivityType=-1&searchType=oha&currentSortSettings=&resultList.currentPageNumber="+str(i)+"&nextList=Next%3E"

    while True:
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'lxml')
            tables = soup.find_all('table', {'class': 'bordertb'})
            break
        except:
            print("Connection error, retrying in 2 seconds...")
            temp += 1
            time.sleep(2)

    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all('td', {'class': 'bgcelllist'})
            row_data = [cell.text.strip().replace('"', '""') for cell in cells]
            if len(row_data) == len(header) and row_data[3] not in existing_ids:
                data.append(row_data)

    # Write the scraped data to a CSV file
    with open('Jega/t.csv', 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        if i == 0:
            writer.writerow(header)
        for row in data:
            writer.writerow(row)

    # Update the set of existing IDs
    existing_ids.update(row[3] for row in data)

    # Clear the data list for the next iteration
    data.clear()
