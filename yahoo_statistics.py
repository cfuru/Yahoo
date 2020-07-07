import pandas as pd
import numpy as np
import re
import pyodbc

import dateutil.parser as dparser
from datetime import datetime

from bs4 import BeautifulSoup

import requests, lxml
from lxml import html


class statistics:
    base_url = "https://finance.yahoo.com/"

    def __init__(self, symbol):
        self.symbol = symbol.upper()
        self.path = "quote/{0}/key-statistics?p={0}".format(symbol)
        self.url = self.base_url + self.path
        self.methods = ['scrape_page', 'label_stats']
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
        self.price_history, self.share_stats, self.trams = table_list
        return self.valuation

    def clean_category_rows(self, df):
        '''
        
        :param df: Takes a dataframe as input
        :return: Returns a dataframe with erased digits from category column rows

        '''
        df['Category'] = df.apply(lambda row: re.sub(r'\d+', '', row['Category']), axis = 1)
        
        return df

    def clean_label_names(self, cols):
        try:
            cols.values[1] = dparser.parse(cols[1], fuzzy=True).strftime("%m/%d/%Y") #Fuzzy logic to find date in text for column naming
        except:
            pass

        cols = list(cols[i] for i in range(1, len(cols)))
        cols.insert(0, 'Category') 
        return cols

    def unit_converter(self, df):
        billion = 1_000_000_000
        million = 1_000_000
        if df[-1] == 'B':
            return float(df[:-1])*billion
        elif df[-1] == 'M':
            return float(df[:-1])*million
        else:
            return float(df)

    def mssql_connection(self):
        pyodbc.pooling = False
        server = 'DESKTOP-F0MM68K'
        database = 'christopherFuru'
        driver= '{SQL Server}'

        cnxn = pyodbc.connect('DRIVER='+driver+ \
                            ';SERVER='+server+ \
                            ';DATABASE='+database + \
                            ';Trusted_Connection=yes')
        cursor = cnxn.cursor()

        return cnxn, cursor

    def insert_mssql(self, df, cursor, cnxn):
        df['Ticker'] = self.symbol
        df = pd.melt(df.reset_index(), id_vars = ['Ticker', 'Category'])

        query_create_temp_table = """
                        CREATE TABLE #Valuations (
                        Ticker VARCHAR(50) NOT NULL,
                        Category VARCHAR(100) NOT NULL,
                        Date DATE NOT NULL,
                        Value MONEY NOT NULL);
                        """
        cursor.execute(query_create_temp_table)
        cursor.commit()

        query_insert_into_temp_table = """
                        INSERT INTO #Valuations VALUES 
                    """
        for i, item in enumerate(df.values.tolist()):
            query_insert_into_temp_table += "('" + str(item[0]) + "','" + str(item[1]) + "','" + str(item[2]) + "','" + str(self.unit_converter(item[3])) +  "')"
            if i < len(df.values.tolist())-1:
                query_insert_into_temp_table += ","
            else:
                query_insert_into_temp_table += ";"
        # print(insert_query)

        cursor.execute(query_insert_into_temp_table)
        cursor.commit()

        query_merge = """
                        MERGE
                            Yahoo.Valuations
                        AS
                            D
                        USING
                        (
                            SELECT * FROM #Valuations
                        ) AS S ON
                            S.Ticker = D.Ticker AND
                            S.Category = D.Category AND
                            S.Date = D.Date
                        WHEN MATCHED AND
                            D.Value <> S.Value
                        THEN UPDATE SET
                            D.Value = S.Value
                        WHEN NOT MATCHED THEN INSERT
                        (
                            Ticker,
                            Category,
                            Date,
                            Value
                        )
                        VALUES
                        (
                            S.Ticker,
                            S.Category,
                            S.Date,
                            S.Value
                        );
        """
        cursor.execute(query_merge)
        cursor.commit()

        cursor.execute("DROP TABLE #Valuations")
        cursor.commit()

        cursor.close()
        cnxn.close()
        print("Done.")

    def __indexLabel__(self, df):
        '''
        
        :param df: Takes a dataframe as input.
        :return: returns a dataframe with cleaned column labels and a set index.
        
        '''
        df.columns = self.clean_label_names(df.columns)
        df = self.clean_category_rows(df)

        # df['Ticker'] = self.symbol
        df = df.set_index('Category')
        df = df.dropna()
        return df

if __name__ == "__main__":
    shopify_stats = statistics('AAK.ST')
    table_list = shopify_stats.scrape_page()
    table_list = shopify_stats.label_stats(table_list)
    cnxn, cursor = shopify_stats.mssql_connection()
    shopify_stats.insert_mssql(table_list, cursor, cnxn)

    # table_list = pd.melt(table_list.reset_index(), id_vars = ['Ticker', 'Category'])

# table_list

# table_list['Category'] = table_list.apply(lambda row: re.sub(r'\d+', '', row['Category']), axis = 1)
# table_list
# re.sub(r'\d+', '', df.columns[i])