from tqdm import tqdm
import datetime
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

# define the header row for the CSV file
header = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]


# Read the existing transaction IDs from the previously scraped CSV file
try:
	df_existing = pd.read_csv('Jega/t.csv')
	# df_existing = pd.read_csv('Transactions55K.csv')
	existing_ids = set(df_existing['4'].unique())
	print("Found {} existing transaction IDs".format(len(existing_ids)))
except FileNotFoundError:
	existing_ids = set()
	print("No existing transaction IDs found")

# Create an empty list to store the scraped data
data = []

# A variable that is used to count the number of times the connection was lost.
temp = 0;

for i in range(0, 2):
	url = "https://ec.europa.eu/clima/ets/oha.do?form=oha&languageCode=fr&accountHolder=&installationIdentifier=&installationName=&permitIdentifier=&mainActivityType=-1&searchType=oha&currentSortSettings=&resultList.currentPageNumber="+str(i)+"&nextList=Next%3E"
	# response = requests.get(url)

	# soup = BeautifulSoup(response.content, 'lxml') 
	# tables = soup.find_all('table', {'id': 'tblAccountSearchResult'})

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

	for i, table in enumerate(tables):
		rows = table.find_all('tr')
		for row in rows:
			cells = row.find_all('td', {'class': 'bgcelllist'})
			row_data = [cell.text.strip() for cell in cells]
			data.append(row_data)

	df = pd.DataFrame(data, columns=header)
	
	df = df.dropna(how='all')
	if i == 0:
		df.to_csv('Jega/t.csv', index=False, encoding='utf-8')
	else:
		df.to_csv('Jega/t.csv', mode='a', header=False, index=False, encoding='utf-8')


