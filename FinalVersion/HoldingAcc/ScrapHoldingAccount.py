import csv
import requests
from bs4 import BeautifulSoup
import	time
from tqdm import tqdm

# Define the header row for the CSV file
header = ["National Administrator", "Account Type", "Account Holder Name", "Installation/Aircraft ID", "Installation Name/Aircraft Operator Code", "Company Registration No", "Permit/Plan ID", "Permit/Plan Date", "Main Activity Type", "Latest Compliance Code"]

# Create an empty list to store the scraped data
data = []
temp = 0


# first_page = lengh of liens.csv
with open('TableauAcc.csv' , encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile)
    row_count_header = len(list(reader))
# print("Number of rows:", row_count_header)
row_count = row_count_header - 1
# print("Number of rows without header:", row_count)
first_page = (row_count) // 20
print("First page: {}".format(first_page))


# Check how many pages are available on the website
url1 = 'https://ec.europa.eu/clima/ets/oha.do?form=oha&languageCode=fr&accountHolder=&installationIdentifier=&installationName=&permitIdentifier=&mainActivityType=-1&search=Search&searchType=oha&currentSortSettings='
response = requests.get(url1)
soup = BeautifulSoup(response.content, 'lxml')
last_page = int(soup.find('input', {'name': 'resultList.lastPageNumber'}).get('value'))
print("Last page: {}".format(last_page))


# Iterate over all pages and retrieve the data
for i in tqdm(range(first_page, last_page)):
	url = f"https://ec.europa.eu/clima/ets/oha.do?form=oha&languageCode=fr&accountHolder=&installationIdentifier=&installationName=&permitIdentifier=&mainActivityType=-1&searchType=oha&currentSortSettings=&resultList.currentPageNumber={i}&nextList=Next>"
	
	
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
			if len(row_data) == len(header):
				data.append(row_data)

# Write the scraped data to a CSV file
with open('TableauAcc.csv', 'w', newline='', encoding='utf-8') as f:
	writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
	writer.writerow(header)
	for row in data:
		writer.writerow(row)
