from bs4 import BeautifulSoup
import requests

# Set the base URL for the website
base_url = "https://ec.europa.eu"

# Set the initial URL
url = "https://ec.europa.eu/clima/ets/transaction.do?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&search=Search&currentSortSettings="

# Set a flag to control the loop
done = False

# Loop until the flag is set to True
while not done:
    # Make a request to the website
    response = requests.get(url)

    # Parse the HTML content
    soup = BeautifulSoup(response.content, "html.parser")

    # Find all tables on the page
    tables = soup.find_all("table")

    # Iterate through the tables and print their data
    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            for cell in cells:
                print(cell.text)

    # Find the "nextList" button
    next_button = soup.find("a", {"title": "Go to the next page"})
    if next_button:
        # If the "nextList" button is found, get its href attribute
        next_url = next_button["href"]
        # Concatenate the base URL and the href attribute to get the full URL
        url = base_url + next_url
    else:
        # If the "nextList" button is not found, set the flag to True to exit the loop
        done = True