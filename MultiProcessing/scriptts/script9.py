import concurrent.futures
import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import datetime
import time

# Define the headers
headers = ["Transaction ID", "Transaction Type", "Transaction Date", "Transaction Status", "Transferring Registry", "Transferring Account Type", "Transferring Account Name", "Transferring Account Identifier", "Transferring Account Holder", "Acquiring Registry", "Acquiring Account Type", "Acquiring Account Name", "Acquiring Account Identifier", "Acquiring Account Holder","Nb of units","Option"]

# Create a directory to store the files
directory = "transactions"
if not os.path.exists(directory):
    os.makedirs(directory)

# Define a function to fetch the data from a page
def fetch_data(page_number):
    url = 'https://ec.europa.eu/clima/ets/transaction.do?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&currentSortSettings=&backList=%3CBack&resultList.currentPageNumber={}'.format(page_number)
    response = requests.get(url)
    # soup = BeautifulSoup(response.content, 'lxml')
    # soup = BeautifulSoup(response.content.decode('UTF-8'), 'lxml')
    soup = BeautifulSoup(response.text, 'lxml')

    all_tables = soup.find_all('table', {'class': 'bordertb'})
    if len(all_tables) >= 2:
        outer_table = all_tables[1]
        inner_table = outer_table.find('table', {'id': 'tblTransactionSearchResult'})
        if inner_table is not None:
            rows = []
            for row in inner_table.find_all('tr')[2:]:
                cells = row.find_all('td')
                rows.append([val.text.strip() for val in cells[:len(cells)-1]])
            rows = [row for row in rows if any(cell.strip() != "" for cell in row)]
            return rows
    return None
start_time = time.time()
# Create a thread pool with 16 workers
with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
    # Use the map function to fetch the data for each page number in parallel
    results = executor.map(fetch_data, range(33841,38071)) #2-4230-8460-12690-16920-21150-25380-29610-33840-38070-42300-46530-50760-55342 add one to the page number
    # print results type   
    print(type(results))

    
    for i, result in enumerate(results):
        if result is not None:
            df = pd.DataFrame(result, columns=headers)
            file_name = f"{directory}/transactions_9.csv"
            df.to_csv(file_name, index=False, mode='a', header=not os.path.exists(file_name))
            

end_time = time.time()
# total_time = end_time - start_time
time_taken = end_time - start_time
time_taken_formatted = str(datetime.timedelta(seconds=time_taken))
print("Time taken: {}".format(time_taken_formatted))


print("I am done scraping the data and merged it into a single file")