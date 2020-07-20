import pyodbc
import pandas as pd

class fundamentals:

    def __init__(self):
        self.driver= '{SQL Server}'
        self.server = 'DESKTOP-F0MM68K'
        self.database = 'christopherFuru'
        self.schema = 'yahoo'
        self.tableNameValuation = 'fundamentals'

    def connect(self):
        cnxn = pyodbc.connect('DRIVER=' + self.driver + \
                            ';SERVER='+ self.server + \
                            ';DATABASE='+ self.database + \
                            ';Trusted_Connection=yes')
        cursor = cnxn.cursor()
        return cnxn, cursor

    def createTable(self, cnxn, cursor):
        query_create_temp_table = f"""
                        CREATE TABLE {self.schema + '.[' + self.tableNameValuation + ']'} 
                        (
                            Ticker      VARCHAR(50) NOT NULL,
                            Category    VARCHAR(100) NOT NULL,
                            Date        DATE NOT NULL,
                            Value       MONEY NOT NULL

                            CONSTRAINT PK_[{self.tableNameValuation}] PRIMARY KEY (Ticker, Category, Date)
                        );
                        """
        try:
            cursor.execute(query_create_temp_table)
            cursor.commit()
        except:
            print('Table already exists!')

    def prepareDf(self, df):
        df = pd.melt(df.reset_index(), id_vars = ['Ticker', 'Category'])
        return df

    def unitConvert(self, data):
        '''

        :param data: Takes a value as input
        :return: Returns converted value 

        '''
        trillion = 1_000_000_000_000
        billion = 1_000_000_000
        million = 1_000_000
        thousand = 1_000
        try:
            if data[-1] == 'T':
                return float(data[:-1])*trillion
            elif data[-1] == 'B':
                return float(data[:-1])*billion
            elif data[-1] == 'M':
                return float(data[:-1])*million
            elif data[-1] == 'k':
                return float(data[:-1])*thousand
            else:
                return float(data)
        except:
            pass

    def merge(self, df, cnxn, cursor):
        df = self.prepareDf(df)

        cursor.execute(f"""IF OBJECT_ID(N'tempdb..#{self.tableNameValuation}') IS NOT NULL
                        DROP TABLE #{self.tableNameValuation};""")
        cursor.commit()

        query_create_temp_table = f"""
                        CREATE TABLE #{self.tableNameValuation} 
                        (
                            Ticker      VARCHAR(50) NOT NULL,
                            Category    VARCHAR(100) NOT NULL,
                            Date        DATE NOT NULL,
                            Value       MONEY NOT NULL

                            --CONSTRAINT PK_#{self.tableNameValuation} PRIMARY KEY (Ticker, Category, Date)
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
                            {self.schema + '.[' + self.tableNameValuation + ']'}
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

        print("SQL-merge done!")


class prices:
    def __init__(self):
        self.driver= '{SQL Server}'
        self.server = 'DESKTOP-F0MM68K'
        self.database = 'christopherFuru'
        self.schema = 'yahoo'
        self.tableNameValuation = 'prices'

    def connect(self):
        cnxn = pyodbc.connect('DRIVER=' + self.driver + \
                            ';SERVER='+ self.server + \
                            ';DATABASE='+ self.database + \
                            ';Trusted_Connection=yes')
        cursor = cnxn.cursor()
        return cnxn, cursor

    def createTable(self, cnxn, cursor):
        query_create_temp_table = f"""
                        CREATE TABLE {self.schema + '.[' + self.tableNameValuation + ']'} 
                        (
                            Date                Date NOT NULL,
                            Ticker              VARCHAR(100) NOT NULL,
                            ClosePriceInSek     MONEY NOT NULL,

                            CONSTRAINT PK_{self.tableNameValuation} PRIMARY KEY (Date, Ticker)
                        );
                        """
        try:
            cursor.execute(query_create_temp_table)
            cursor.commit()
        except:
            print('Table already exists!')

    def merge(self, df, cnxn, cursor):
        cursor.execute(f"""IF OBJECT_ID(N'tempdb..#{self.tableNameValuation}') IS NOT NULL
                        DROP TABLE #{self.tableNameValuation};""")
        cursor.commit()

        query_create_temp_table = f"""
                        CREATE TABLE #{self.tableNameValuation} 
                        (
                            Date                Date NOT NULL,
                            Ticker              VARCHAR(100) NOT NULL,
                            ClosePriceInSek     MONEY NOT NULL,

                            CONSTRAINT PK_#{self.tableNameValuation} PRIMARY KEY (Date, Ticker)
                        );
                        """
        cursor.execute(query_create_temp_table)
        cursor.commit()

        query_insert_into_temp_table = f"""
                        INSERT INTO #{self.tableNameValuation} VALUES 
                    """
        for i, item in enumerate(df.values.tolist()):
            query_insert_into_temp_table += "('" + str(item[0]) + "','" + str(item[2]) + "','" + str(item[1]) +  "')"
            if i < len(df.values.tolist())-1:
                query_insert_into_temp_table += ","
            else:
                query_insert_into_temp_table += ";"

        cursor.execute(query_insert_into_temp_table)
        cursor.commit()

        query_merge = f"""
                        MERGE
                            {self.schema + '.[' + self.tableNameValuation + ']'}
                        AS
                            D
                        USING
                        (
                            SELECT * FROM #{self.tableNameValuation}
                        ) AS S ON
                            S.Date = D.Date AND
                            S.Ticker = D.Ticker
                        WHEN MATCHED AND
                            D.ClosePriceInSek <> S.ClosePriceInSek
                        THEN UPDATE SET
                            D.ClosePriceInSek = S.ClosePriceInSek
                        WHEN NOT MATCHED THEN INSERT
                        (
                            Date,
                            Ticker,
                            ClosePriceInSek
                        )
                        VALUES
                        (
                            S.Date,
                            S.Ticker,
                            S.ClosePriceInSek
                        );
        """
        cursor.execute(query_merge)
        cursor.commit()

        print("SQL-merge done!")