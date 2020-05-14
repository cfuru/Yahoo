import pandas as pd
import numpy as np

from bs4 import BeautifulSoup

import requests, lxml
from lxml import html


class statistics:
    base_url = "https://finance.yahoo.com/"

        def __init__(self, symbol):
        '''

        :param symbol: stock symbol in all caps
        please note that any Canadian TSX stocks are followed with ".TO" ... check the relevant URL for formatting.
        '''
        self.symbol = symbol.upper()
        self.path = "quote/{0}/key-statistics?p={0}".format(symbol)
        self.url = self.base_url + self.path
        self.methods = ['scrape_page', 'label_stats']
        self.attributes = ['self.symbol', 'self.path', 'self.url','self.methods', 'self.hdrs', \
                           'self.valuation', 'self.fiscal_year', \
                           'self.profitability', 'self.manager_effect', \
                            'self.income_statement', 'self.balance_sheet', 'self.cash_statement', \
                            'self.price_history', 'self.share_stats', 'self.dividendSplit']
        self.hdrs = {"authority": "finance.yahoo.com",
                     "method": "GET",
                     "path": self.path,
                     "scheme": "https",
                     "accept": "text/html,application/xml;q=0.9",
                     "accept-encoding": "gzip, deflate, br",
                     "accept-language": "en-US,en;q=0.9",
                     "referer": self.base_url,
                     "sec-fetch-mode": "navigate",
                     "sec-fetch-site": "same-origin",
                     "sec-fetch-user": "?1",
                     "upgrade-insecure-requests": "1",
                     "user-agent": "Mozilla/5.0 (Windows NT 10.0;)"}

    def scrape_page(self):
        '''

        :return: scrapes the content of the class URL,
                   using headers defined in the init function,
                   returning a byte string of html code.
        '''
        page = requests.get(self.url, headers=self.hdrs)
        soup = BeautifulSoup(page.content, 'lxml')
        tables = soup.find_all('table')
        iterator = range(0, len(tables))
        function = lambda x: pd.read_html(str(tables[x]))
        table_list = list(map(function, iterator))
        return table_list

    def label_stats(self, table_list):
        '''

        :param table_list: uses the output of the scrape_page method
        :return: creates attributes for the statistics class object,
                 uses indexLabel method to label columns and set the dataframes' index
        
        '''
        iterator = [table_list[i][0] for i in range(0, len(table_list))]
        table_list = list(map(lambda df: self.__indexLabel__(df), iterator))
        self.valuation, self.fiscal_year, self.profitability, self.manager_effect, \
        self.income_statement, self.balance_sheet, self.cash_statement, \
        self.price_history, self.share_stats, self.dividendSplit = table_list
        return table_list

    def __indexLabel__(self, df):
        '''
        
        :param df: Takes a dataframe as input.
        :return: returns a dataframe with column labels and a set index.
        
        '''
        df.columns = ['Measure', 'Value']
        df = df.set_index('Measure')
        return df

if __name__ == "__main__":
    shopify_stats = statistics('SHOP')
    table_list = shopify_stats.scrape_page()
    table_list = shopify_stats.label_stats(table_list)