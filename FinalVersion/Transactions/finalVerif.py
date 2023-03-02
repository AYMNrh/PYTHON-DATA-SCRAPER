from tqdm import tqdm
import datetime
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

# Define the headers
headers = ["Transaction ID", "Transaction Type", "Transaction Date", "Transaction Status", "Transferring Registry", "Transferring Account Type", "Transferring Account Name", "Transferring Account Identifier", "Transferring Account Holder", "Acquiring Registry", "Acquiring Account Type", "Acquiring Account Name", "Acquiring Account Identifier", "Acquiring Account Holder","Nb of units","Option"]

# Read the existing transaction IDs from the previously scraped CSV file
try:
	df_existing = pd.read_csv('TransactionsSansVerif.csv')
	# df_existing = pd.read_csv('Transactions55K.csv')
	existing_ids = set(df_existing['Transaction ID'].unique())
	print("Found {} existing transaction IDs".format(len(existing_ids)))
except FileNotFoundError:
	existing_ids = set()
	print("No existing transaction IDs found")



# first_page = 5
# last_page = 8

# Check how many pages from file to determine where to start scraping
# range is now (first_page, last_page)
url1 = 'https://ec.europa.eu/clima/ets/transaction.do?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&currentSortSettings=&backList=%3CBack&resultList.currentPageNumber=1'
response = requests.get(url1)
soup = BeautifulSoup(response.content, 'lxml')
# Get the value of the resultList.lastPageNumber input
number_of_ids = len(existing_ids)
number_of_pages = number_of_ids//20
first_page = number_of_pages
print("First page: {}".format(first_page))
last_page = int(soup.find('input', {'name': 'resultList.lastPageNumber'}).get('value'))
print("Last page: {}".format(last_page))
last_new_page =  last_page + 1



# Create an empty list to store the scraped data
data = []
start_time = time.time()

# A variable that is used to count the number of times the connection was lost.
temp = 0;

# Loop through the pages
for i in tqdm(range(first_page, last_new_page)): #2-55342 add one to the page number
	url = 'https://ec.europa.eu/clima/ets/transaction.do?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&currentSortSettings=&backList=%3CBack&resultList.currentPageNumber={}'.format(i)
	
	while True:
		try:
			response = requests.get(url)
			soup = BeautifulSoup(response.content, 'lxml')
			all_tables = soup.find_all('table', {'class': 'bordertb'})
			break
		except:
			print("Connection error, retrying in 2 seconds...")
			temp += 1
			time.sleep(2)

	# Check if the inner table exists
	if len(all_tables) >= 2:
		outer_table = all_tables[1]
		inner_table = outer_table.find('table', {'id': 'tblTransactionSearchResult'})
		if inner_table is not None:
			rows = []
			for row in inner_table.find_all('tr')[2:]:
				cells = row.find_all('td')
				row_data = [val.text.strip() for val in cells]
				# Check if the transaction ID is already in the existing IDs set
				if row_data and row_data[0] not in existing_ids:
					if row_data[0] == "Details":
						continue
					# print("New transaction ID found: {}".format(row_data[0]))
					rows.append(row_data[:-1]) # Remove the last element before appending
					existing_ids.add(row_data[0]) # Add the new ID to the existing IDs set
				elif row_data[0] == "Details":
					continue
				else:
					# print("Transaction ID already exists: {}".format(row_data[0]))
					continue
			data.extend(rows)
		else:
			print("Table not found")
	else:
		print("Table not found")

# Save the data to a CSV file
try:
	df = pd.DataFrame(data, columns=headers)
	# remove empty rows
	df = df.dropna(how='all')
	file_name = f"TransactionsSansVerif.csv"
	df.to_csv(file_name, index=False, mode='a', header=not os.path.exists(file_name))
except ValueError:
	df = pd.DataFrame(columns=headers)
	print("No new data found")

print("I am done scraping the data")
end_time = time.time()
time_taken = end_time - start_time
print("Time taken:", str(datetime.timedelta(seconds=time_taken)))
# temp is the number of times the connection was lost
print("Number of times the connection was lost:", temp)
