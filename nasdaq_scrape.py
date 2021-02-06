import pandas as pd
from pandas_datareader import data as web
import numpy as np
import re


import dateutil.parser as dparser
from datetime import datetime

from bs4 import BeautifulSoup

import requests, lxml
from lxml import html

class companies:
    base_url = "http://www.nasdaqomxnordic.com/aktier/listed-companies/"

    def __init__(self, list):
        self.path = list
        self.url = self.base_url + self.path

        self.hdrs = {'Connection': 'keep-alive',
                    'method': 'GET',
                    'scheme': 'https',
                    'Expires': '-1',
                    'Upgrade-Insecure-Requests': '1',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) \
                        AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36'
                   }

    def scrape(self):
        '''
        :return: scrapes the content of the class URL,
                   using headers defined in the init function,
                   returning a byte string of html code.
        '''
        page = requests.get(self.url, headers = self.hdrs)
        soup = BeautifulSoup(page.content, 'lxml')
        # tables = soup.find_all('table')
        print(soup)
        # iterator = range(0, len(tables))
        # function = lambda x: pd.read_html(str(tables[x]))
        # table_list = list(map(function, iterator))
        return soup