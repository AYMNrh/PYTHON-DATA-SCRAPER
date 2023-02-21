import requests
from bs4 import BeautifulSoup
import pandas as pd

# Make a request to the website
# url = 'https://ec.europa.eu/clima/ets/transaction.do?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&search=Search&currentSortSettings='
for i in range(0, 11):
    url = 'https://ec.europa.eu/clima/ets/transaction.do?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&currentSortSettings=&resultList.currentPageNumber={}&nextList=Next%3E'.format(i)
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
            rows = []
            # define headers
            # th doesnt exist in the html, they are in a ,  so we must define the headers beforhand
            headers = ["Transaction ID", "Transaction Type", "Transaction Date", "Transaction Status", "Transferring Registry", "Transferring Account Type", "Transferring Account Name", "Transferring Account Identifier", "Transferring Account Holder", "Acquiring Registry", "Acquiring Account Type", "Acquiring Account Name", "Acquiring Account Identifier", "Acquiring Account Holder","Nb of units","Option"]
            # for row in inner_table.find_all('tr')[3:]:
            #     cells = row.find_all('td')
            #     rows.append([val.text for val in cells[:len(cells)-1]])
            for row in inner_table.find_all('tr')[2:]: #we start a the third tr because the other ones dont contain data we want to scrap
                cells = row.find_all('td')
                rows.append([val.text.strip() for val in cells[:len(cells)-1]])
            rows = [row for row in rows if any(cell.strip() != "" for cell in row)]
            # Create a pandas DataFrame with the extracted data
            df = pd.DataFrame(rows, columns=headers)
            # Save the DataFrame to a CSV file
        else:
            print("inner table not found")

    else:
        print("table not found")
        
    df = pd.DataFrame(rows, columns=headers)
    df.to_csv('transactions9.csv', mode='a', header=False, index=False)
