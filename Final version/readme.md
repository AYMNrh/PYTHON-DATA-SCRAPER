<!-- read me file for finalVerif.py	-->
# Final Version of the verification script
# This script is used to scrap info from the website and verify the data

# Importing the libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
import datetime
import os


# Define the headers
# Read the existing transaction IDs from the previously scraped CSV file
# Check how many pages from file to determine where to start scraping
# Define the URL
# Loop through the pages
# Verify the data and skip the transaction IDs that are already in the CSV file 
# Get the page content
# Save the data to a CSV file


