import pandas as pd
import numpy as np
import re
import pyodbc

import dateutil.parser as dparser
from datetime import datetime

from bs4 import BeautifulSoup

import requests, lxml
from lxml import html

class yahooMsSqlServer:

    def __init__(self):
        self.driver= '{SQL Server}'
        self.server = 'DESKTOP-F0MM68K'
        self.database = 'christopherFuru'
        self.schema = 'yahoo'
        self.tableNameValuation = 'Valuations'

    def connect(self):
        cnxn = pyodbc.connect('DRIVER=' + self.driver + \
                            ';SERVER='+ self.server + \
                            ';DATABASE='+ self.database + \
                            ';Trusted_Connection=yes')
        cursor = cnxn.cursor()
        return cnxn, cursor

    def createValuationTable(self, cnxn, cursor):
        query_create_temp_table = f"""
                        CREATE TABLE {self.schema + '.' + self.tableNameValuation} 
                        (
                            Ticker      VARCHAR(50) NOT NULL,
                            Category    VARCHAR(100) NOT NULL,
                            Date        DATE NOT NULL,
                            Value       MONEY NOT NULL

                            CONSTRAINT PK_{self.tableNameValuation} PRIMARY KEY (Ticker, Category, Date)
                        );
                        """
        try:
            cursor.execute(query_create_temp_table)
            cursor.commit()
        except:
            print('Table already exists!')

    def prepareDataframe(self, df):
        df = pd.melt(df.reset_index(), id_vars = ['Ticker', 'Category'])
        return df

    def unitConvert(self, data):
        '''

        :param data: Takes a value as input
        :return: Returns converted value 

        '''
        billion = 1_000_000_000
        million = 1_000_000
        if data[-1] == 'B':
            return float(data[:-1])*billion
        elif data[-1] == 'M':
            return float(data[:-1])*million
        else:
            return float(data)

    def insertIntoValuationTable(self, df, cnxn, cursor):
        df = self.prepareDataframe(df)

        query_create_temp_table = f"""
                        CREATE TABLE #{self.tableNameValuation} 
                        (
                            Ticker      VARCHAR(50) NOT NULL,
                            Category    VARCHAR(100) NOT NULL,
                            Date        DATE NOT NULL,
                            Value       MONEY NOT NULL

                            CONSTRAINT PK_#{self.tableNameValuation} PRIMARY KEY (Ticker, Category, Date)
                        );
                        """
        cursor.execute(query_create_temp_table)
        cursor.commit()

        query_insert_into_temp_table = f"""
                        INSERT INTO #{self.tableNameValuation} VALUES 
                    """
        for i, item in enumerate(df.values.tolist()):
            query_insert_into_temp_table += "('" + str(item[0]) + "','" + str(item[1]) + "','" + str(item[2]) + "','" + str(self.unitConvert(item[3])) +  "')"
            if i < len(df.values.tolist())-1:
                query_insert_into_temp_table += ","
            else:
                query_insert_into_temp_table += ";"

        cursor.execute(query_insert_into_temp_table)
        cursor.commit()

        query_merge = f"""
                        MERGE
                            {self.schema + '.' + self.tableNameValuation}
                        AS
                            D
                        USING
                        (
                            SELECT * FROM #{self.tableNameValuation}
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

        cursor.execute(f"DROP TABLE #{self.tableNameValuation}")
        cursor.commit()

        cursor.close()
        cnxn.close()
        print("Done.")

#########################################################################################

class statistics:
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
        page = requests.get(self.url, headers=self.hdrs)
        soup = BeautifulSoup(page.content, 'lxml')
        tables = soup.find_all('table')
        iterator = range(0, len(tables))
        function = lambda x: pd.read_html(str(tables[x]))
        table_list = list(map(function, iterator))
        return table_list

    def labelTables(self, table_list):
        '''
        :param table_list: uses the output of the scrape method
        :return: creates attributes for the statistics class object,
                 uses indexLabel method to label columns and set the dataframes' index
        
        '''

        iterator = [table_list[i][0] for i in range(0, len(table_list))]
        table_list = list(map(lambda df: self.__indexLabel__(df), iterator))
        self.valuation, self.fiscal_year, self.profitability, self.manager_effect, \
        self.income_statement, self.balance_sheet, self.cash_statement, \
        self.price_history, self.share_stats, self.trams = table_list
        return self.valuation#table_list

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

if __name__ == "__main__":
    shopify_stats = statistics('AAK.ST')
    table_list = shopify_stats.scrape()
    table_list = shopify_stats.labelTables(table_list)
    sql = yahooMsSqlServer()
    cnxn, cursor = sql.connect()
    # sql.createValuationTable(cnxn, cursor)
    sql.insertIntoValuationTable(table_list, cnxn, cursor)
