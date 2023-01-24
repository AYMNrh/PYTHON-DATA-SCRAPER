# import requests
# from bs4 import BeautifulSoup
# import pandas as pd

# # Make a request to the website
#  url = 'https://ec.europa.eu/clima/ets/transaction.do?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&search=Search&currentSortSettings='
# response = requests.get(url)

# # Parse the HTML content
# soup = BeautifulSoup(response.content, 'html.parser')

# # Find the table with the specified id
# table = soup.find('table', {'id': 'tblTransactionSearchResult'})

# # Extract the table headers and data rows
# headers = [header.text for header in table.find_all('th')]
# rows = []
# for row in table.find_all('tr'):
#     rows.append([val.text for val in row.find_all('td')])

# # Create a pandas DataFrame with the extracted data
# df = pd.DataFrame(rows, columns=headers)

# # Save the DataFrame to a CSV file
# df.to_csv('transactions.csv', index=False)
import requests
from bs4 import BeautifulSoup
import pandas as pd

# Make a request to the website
# url = 'https://www.example.com/transactions'
url = 'https://ec.europa.eu/clima/ets/transaction.do?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&search=Search&currentSortSettings='

response = requests.get(url)

# Parse the HTML content
soup = BeautifulSoup(response.content, 'html.parser')

# Find the table with the specified id
table = soup.find('div', {'id': 'tblTransactionSearchResult'})


# Extract the table headers and data rows
headers = [header.text for header in table.find_all('tr')[1].find_all('td')]
rows = []
for row in table.find_all('tr')[1:]:
    rows.append([val.text for val in row.find_all('td')])

# Create a pandas DataFrame with the extracted data
df = pd.DataFrame(rows, columns=headers)

# Save the DataFrame to a CSV file
df.to_csv('transactions.csv', index=False)
