import concurrent.futures
import pandas as pd
import requests
from bs4 import BeautifulSoup
import datetime
import time
from tqdm import tqdm

# Define the headers
headers = ["Transaction ID", "Transaction Type", "Transaction Date", "Transaction Status", "Transferring Registry", "Transferring Account Type", "Transferring Account Name", "Transferring Account Identifier", "Transferring Account Holder", "Acquiring Registry", "Acquiring Account Type", "Acquiring Account Name", "Acquiring Account Identifier", "Acquiring Account Holder","Nb of units","Option"]

# Create an empty list to store the scraped data
data = []

# Define a function to fetch the data from a page
def fetch_data(page_number):
    url = 'https://ec.europa.eu/clima/ets/transaction.do?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&currentSortSettings=&backList=%3CBack&resultList.currentPageNumber={}'.format(page_number)
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'lxml')
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
# Create a thread pool with 8 workers
start_time = time.time()
with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
    # Use the map function to fetch the data for each page number in parallel
    results = executor.map(fetch_data, tqdm(range(2, 22)))
    # Iterate over the results and append the data to the list
    for result in results:
        if result is not None:
            data.append(result)

# Concat the list into a DataFrame
df = pd.concat([pd.DataFrame(i, columns=headers) for i in data], ignore_index=True)

# Save the DataFrame to a CSV file
df.to_csv('multi3.csv', index=False)
print("I am done scraping the data")
end_time = time.time()
# total_time = end_time - start_time
time_taken = end_time - start_time
time_taken_formatted = str(datetime.timedelta(seconds=time_taken))
print("Time taken: {}".format(time_taken_formatted))
