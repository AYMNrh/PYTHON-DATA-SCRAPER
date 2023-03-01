from tqdm import tqdm
import datetime
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os


#headers = ["A", "B", "C", "D", "E", "F"]

#data = []
#start_time = time.time()
for i in range (0,3):

 url = "https://ec.europa.eu/clima/ets/oha.do?form=oha&languageCode=fr&accountHolder=&installationIdentifier=&installationName=&permitIdentifier=&mainActivityType=-1&searchType=oha&currentSortSettings=&resultList.currentPageNumber="+str(i)+"&nextList=Next%3E"
 response = requests.get(url)

 soup = BeautifulSoup(response.content, 'lxml') 
 tables = soup.find_all('table', {'id': 'tblAccountSearchResult'})
 links = []


 for table in tables:
    rows = table.find_all('tr')
    for row in rows:
        count = 0
        cells = row.find_all('td')
        for cell in cells:
            for a in cell.find_all('a', class_='listlink', href=True):
                count += 1
                if count == 2:
                    if a['href'] not in links:
                        links.append(a['href'])
                    break
            if count == 2:
                break


with open('links.txt', 'a') as file:
    for link in links:
        file.write(link + '\n')

