from tqdm import tqdm
import datetime
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from multiprocessing import Pool

# Define the headers
headers = ["Transaction ID", "Transaction Type", "Transaction Date", "Transaction Status", "Transferring Registry", "Transferring Account Type", "Transferring Account Name", "Transferring Account Identifier", "Transferring Account Holder", "Acquiring Registry", "Acquiring Account Type", "Acquiring Account Name", "Acquiring Account Identifier", "Acquiring Account Holder","Nb of units","Option"]

# Create an empty list to store the scraped data
data = []
start_time = time.time()

def scrape_page(page_num):
    # Make a request to the website
    url = 'https://ec.europa.eu/clima/ets/transaction.do?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&currentSortSettings=&backList=%3CBack&resultList.currentPageNumber={}'.format(page_num)
    response = requests.get(url)

    # Parse the HTML content
    soup = BeautifulSoup(response.content, 'lxml')

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

# Use a Pool to scrape multiple pages in parallel
if __name__ == '__main__':
    with Pool() as p:
        r = list(tqdm(p.imap(scrape_page, range(2,20)), total=19))

#concat the list into a Dataframe
df = pd.concat([pd.DataFrame(i, columns=headers) for i in data], ignore_index=True)

# Save the DataFrame to a CSV file
df.to_csv('TransactionsFinal55000pages.csv', index=False)

# Print a message indicating that the scraping is complete
print("I am done scraping the data")

# Calculate the total time taken
end_time = time.time()
total_time = end_time - start_time
print("Time taken: ", total_time)

