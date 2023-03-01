import requests
from bs4 import BeautifulSoup



with open('links.txt', 'r') as f:
    urls = f.readlines()


with open('Jega/tableau.csv', 'w', encoding='utf-8') as out_file:
    for url in urls:
        url = url.strip()  # remove whitespace characters (e.g. \n)
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'lxml')
        tables = soup.find_all('table', {'id': 'tblChildDetails'})
        for table in tables:
            cells = table.find_all('td', {'class': 'bgcelllist'})
            for cell in cells[:-14]:
                span = cell.find('span', {'class': 'classictext'})
                out_file.write(span.text.strip() + ',')
        out_file.write('\n')  # add newline after each URL's data



