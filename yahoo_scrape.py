import pandas as pd
from pandas_datareader import data as web
import numpy as np
import re


import dateutil.parser as dparser
from datetime import datetime

from bs4 import BeautifulSoup

import requests, lxml
from lxml import html

#########################################################################################

class fundamentals:
    base_url = "https://finance.yahoo.com/"

    def __init__(self, symbol):
        self.symbol = symbol.upper()
        self.path = "quote/{0}/key-statistics?p={0}".format(symbol)
        self.url = self.base_url + self.path
        self.methods = ['scrape', 'labelTables']
        self.attributes = ['self.symbol', 'self.path', 'self.url','self.methods', 'self.hdrs', \
                            'self.valuation', 'self.fiscal_year', \
                            'self.profitability', 'self.manager_effect', \
                            'self.income_statement', 'self.balance_sheet', 'self.cash_statement', \
                            'self.price_history', 'self.share_stats', 'self.dividendSplit']
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
        tables = soup.find_all('table')
        iterator = range(0, len(tables))
        function = lambda x: pd.read_html(str(tables[x]))
        table_list = list(map(function, iterator))
        print(f'Scraping {self.symbol} using url: {self.url}')
        return table_list

    def labelTables(self, table_list):
        '''
        :param table_list: uses the output of the scrape method
        :return: creates attributes for the statistics class object,
                 uses indexLabel method to label columns and set the dataframes' index
        
        '''

        iterator = [table_list[i][0] for i in range(0, len(table_list))]
        table_list = list(map(lambda df: self.__indexLabel__(df), iterator))
        return table_list

    def cleanCategoryRows(self, df):
        '''
        
        :param df: Takes a dataframe as input
        :return: Returns a dataframe with erased digits from category column rows

        '''
        df['Category'] = df.apply(lambda row: re.sub(r'\d+', '', row['Category']), axis = 1)
        
        return df

    def cleanDateCol(self, cols):
        '''

        :param cols: Takes list of column names
        :return: Returns list of a list of new column names

        '''
        try:
            cols.values[1] = dparser.parse(cols[1], fuzzy=True).strftime("%m/%d/%Y") #Fuzzy logic to find date in text for column naming
        except:
            pass

        cols = list(cols[i] for i in range(1, len(cols)))
        cols.insert(0, 'Category') 
        return cols

    def __indexLabel__(self, df):
        '''
        
        :param df: Takes a dataframe as input.
        :return: returns a dataframe with cleaned column labels and a set index.
        
        '''
        df.columns = self.cleanDateCol(df.columns)
        df = self.cleanCategoryRows(df)

        df['Ticker'] = self.symbol
        df = df.set_index('Category')
        df = df.dropna()
        return df

class prices:
    
    def __init__(self, symbol, startDate, endDate):
        self.dataSource = 'yahoo'
        self.symbol = symbol.upper()
        self.startDate = startDate
        self.endDate = endDate

    def getAdjClose(self):
        try:
            dailyPrices = web.DataReader(
                self.symbol, 
                data_source = self.dataSource, 
                start = self.startDate, 
                end = self.endDate)
            dailyPrices['Ticker'] = self.symbol
            return dailyPrices[['Adj Close', 'Ticker']].reset_index()
        except: 
            print(f'Could not fetch data for {self.symbol}')
            pass
        
        

        