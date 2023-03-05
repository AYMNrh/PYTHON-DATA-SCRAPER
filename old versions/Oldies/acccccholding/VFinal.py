from tqdm import tqdm
import datetime
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd


# Define the headers
headers = ["Transaction ID", "Transaction Type", "Transaction Date", "Transaction Status", "Transferring Registry", "Transferring Account Type", "Transferring Account Name", "Transferring Account Identifier", "Transferring Account Holder", "Acquiring Registry", "Acquiring Account Type", "Acquiring Account Name", "Acquiring Account Identifier", "Acquiring Account Holder","Nb of units","Option"]

# Create an empty list to store the scraped data
data = []
start_time = time.time()
# Loop through the pages
for i in tqdm(range(2,22)):#we have to start at 2 because the first page is labeled 1 and 2 so that we wouldnt get the same data twice, and we have to end in +1 because of how range works
    # Make a request to the website
    # url = 'https://ec.europa.eu/clima/ets/transaction.do?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&currentSortSettings=&resultList.currentPageNumber={}&nextList=Next%3E'.format(i)
    url = 'https://ec.europa.eu/clima/ets/transaction.do?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&currentSortSettings=&backList=%3CBack&resultList.currentPageNumber={}'.format(i)
    response = requests.get(url)

    # Parse the HTML content
    # soup = BeautifulSoup(response.content, 'html.parser')
    soup = BeautifulSoup(response.content, 'lxml') #faster than html.parser

    # Find all elements with the class name "bordertb"
    all_tables = soup.find_all('table', {'class': 'bordertb'})

    # Check if at least two tables exist with class named bordertb
    if len(all_tables) >= 2:
        # Get the second table
        outer_table = all_tables[1]
        #Find the inner table by its id
        inner_table = outer_table.find('table', {'id': 'tblTransactionSearchResult'})

        # Check if inner table exists
        if inner_table is not None:
            rows = []

            for row in inner_table.find_all('tr')[2:]:  #we start a the third tr because the other ones dont contain data we want to scrap
                cells = row.find_all('td')
                rows.append([val.text.strip() for val in cells[:len(cells)-1]])
            rows = [row for row in rows if any(cell.strip() != "" for cell in row)]
            
            # Append the scraped data to the list
            data.append(rows)
        else:
            print("inner table not found") #for debugging

    else:
        print("table not found") #for debugging

#concat the list into a Dataframe
df = pd.concat([pd.DataFrame(i, columns=headers) for i in data], ignore_index=True)

# Save the DataFrame to a CSV file
df.to_csv('TransactionsFinal100pages.csv', index=False)
print("I am done scraping the data")
end_time = time.time()
# total_time = end_time - start_time
time_taken = end_time - start_time
time_taken_formatted = str(datetime.timedelta(seconds=time_taken))
print("Time taken: {}".format(time_taken_formatted))
# print("Total time taken: ", time_taken)
