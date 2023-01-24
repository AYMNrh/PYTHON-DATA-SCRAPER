import scrapy
from tqdm import tqdm
import pandas as pd

class TransactionsSpider(scrapy.Spider):
    name = "transactions"
    headers = ["Transaction ID", "Transaction Type", "Transaction Date", "Transaction Status", "Transferring Registry", "Transferring Account Type", "Transferring Account Name", "Transferring Account Identifier", "Transferring Account Holder", "Acquiring Registry", "Acquiring Account Type", "Acquiring Account Name", "Acquiring Account Identifier", "Acquiring Account Holder","Nb of units","Option"]
    data = []
    start_urls = ['https://ec.europa.eu/clima/ets/transaction.do?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&currentSortSettings=&backList=%3CBack&resultList.currentPageNumber={}'.format(i) for i in range(2,22)]

    def parse(self, response):
        all_tables = response.xpath('//table[@class="bordertb"]')
        if len(all_tables) >= 2:
            outer_table = all_tables[1]
            inner_table = outer_table.xpath('.//table[@id="tblTransactionSearchResult"]')
            if inner_table:
                rows = []
                for row in inner_table.xpath('.//tr')[2:]:
                    cells = row.xpath('.//td/text()').getall()
                    rows.append([val.strip() for val in cells])
                rows = [row for row in rows if any(cell.strip() != "" for cell in row)]
                self.data.append(rows)
            else:
                print("inner table not found")
        else:
            print("table not found")

def closed(self, reason):
    df = pd.concat([pd.DataFrame(i, columns=self.headers) for i in self.data], ignore_index=True)
    df.to_csv('scrapy1.csv', index=False)
    print("I am done scraping the data")
