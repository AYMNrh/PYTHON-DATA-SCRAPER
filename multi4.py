import aiohttp
import asyncio
import async_timeout
import concurrent.futures
import pandas as pd
import requests
from bs4 import BeautifulSoup
import datetime
import time
from tqdm import tqdm

headers = ["Transaction ID", "Transaction Type", "Transaction Date", "Transaction Status", "Transferring Registry", "Transferring Account Type", "Transferring Account Name", "Transferring Account Identifier", "Transferring Account Holder", "Acquiring Registry", "Acquiring Account Type", "Acquiring Account Name", "Acquiring Account Identifier", "Acquiring Account Holder","Nb of units","Option"]

# Create an empty list to store the scraped data
data = []

async def fetch_data(session, page_number):
    url = 'https://ec.europa.eu/clima/ets/transaction.do?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&currentSortSettings=&backList=%3CBack&resultList.currentPageNumber={}'.format(page_number)
    async with async_timeout.timeout(10):
        async with session.get(url) as response:
            soup = BeautifulSoup(await response.text(), 'lxml')
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
async def run():
    async with aiohttp.ClientSession() as session:
        # Use a rate limiter to limit the number of requests per second
        sem = asyncio.Semaphore(5)  # rate limit of 5 requests per second
        tasks = []
        for page_number in range(2, 101):
            tasks.append(asyncio.ensure_future(fetch_data(session, page_number)))
        for task in asyncio.as_completed(tasks):
            await sem.acquire()
            result = await task
            sem.release()
            if result is not None:
                data.append(result)
    df = pd.concat([pd.DataFrame(i, columns=headers) for i in data], ignore_index=True)
    df.to_csv('multi4.csv', index=False)
    print("I am done scraping the data")

loop = asyncio.get_event_loop()
loop.run_until_complete(run())
end_time = time.time()
# total_time = end_time - start_time
time_taken = end_time - start_time
time_taken_formatted = str(datetime.timedelta(seconds=time_taken))
print("Time taken: {}".format(time_taken_formatted))
