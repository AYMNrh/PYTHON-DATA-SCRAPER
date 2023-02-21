import requests
from bs4 import BeautifulSoup
import pandas as pd

# Make a request to the website
url = 'https://ec.europa.eu/clima/ets/transaction.do?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&search=Search&currentSortSettings='
response = requests.get(url)

# Parse the HTML content
soup = BeautifulSoup(response.content, 'html.parser')

# Find all elements with the class name "bordertb"
all_tables = soup.find_all('table', {'class': 'bordertb'})

# Check if at least two tables exist
if len(all_tables) >= 2:
    # Get the second table
    outer_table = all_tables[1]
    #Find the inner table by its id
    inner_table = outer_table.find('table', {'id': 'tblTransactionSearchResult'})
    # Check if inner table exists
    if inner_table is not None:
        # Extract the headers from the third 'tr' element of the inner table
        headers = [header.text for header in inner_table.find_all('tr')[1].find_all('td')]
        rows = []
        # Extract the rows from the inner table that starts from the fourth 'tr' element
        for row in inner_table.find_all('tr')[3:]:
            rows.append([val.text for val in row.find_all('td')])
        # Create a pandas DataFrame with the extracted data
        df = pd.DataFrame(rows, columns=headers)
        # Save the DataFrame to a CSV file
        df.to_csv('transactions.csv', index=False)
    else:
        print("inner table not found")
else:
    print("table not found")
