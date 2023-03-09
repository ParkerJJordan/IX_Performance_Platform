# Various Apen Queries
import pandas as pd
import numpy as np
import pyodbc
import warnings
warnings.filterwarnings('ignore')

class AspenConn:
    def __init__(self, server, username):
        self.server = server
        self.username = username
        self.conn = self.get_conn()

    def get_conn(self):
        '''Establish OPC Connection to Aspen Server'''
        conn = pyodbc.connect(
            f'DRIVER={{AspenTech SQLplus}};HOST={self.server}')
        return conn

    def start_end(self, tag_list, start_datetime, end_datetime, request=1):
        '''Query the Aspen db from the start time to the end time'''
        tag_list = self.parse_tag_list(tag_list)

        sql_query = f'''
        SELECT *
        FROM HISTORY
        WHERE NAME IN {tag_list} AND
        TS BETWEEN TIMESTAMP'{start_datetime}' AND TIMESTAMP'{end_datetime}' AND
        REQUEST = {request}
        '''

        df = (pd.read_sql(sql_query, self.conn).pivot(index='TS',
                                                      columns='NAME',
                                                      values='VALUE'))

        return df
    
    def after(self, tag_list, date_limit, request=5, pivot=True):
        '''Query the Aspen db from the start time to the end time'''
        # tag_list = self.parse_tag_list(tag_list)
        tag_list = list(set(tag_list))
        datalist = []

        for tag in tag_list:
            sql_query = f'''
            SELECT *
            FROM HISTORY
            WHERE NAME IN ('{tag}') AND
            TS >= TIMESTAMP'{date_limit}' AND
            REQUEST = {request}
            '''

            if pivot is True:
                df = (pd.read_sql(sql_query, self.conn).pivot_table(index=['TS'], columns=['NAME'], values='VALUE', aggfunc={'VALUE':np.sum}))
                print(f'Tags pulled: {list(df.columns)}')
            else:
                df = (pd.read_sql(sql_query, self.conn))
            datalist.append(df)
        tagdata = pd.concat(datalist)
        
        return tagdata.reset_index()

    def current(self, tag_list, mins=30, hours=0, days=0, period=0, request=1, pivot=True):
        '''Query the Aspen db from the current time back'''
        # tag_list = self.parse_tag_list(tag_list)
        tag_list = list(set(tag_list))
        datalist = []
        duration = 10 * 60 * (mins + 60 * hours + 24 * 60 * days)

        print(f'Quering tags {tag_list}')
        for tag in tag_list:
            if period > 0:
                sql_query = f'''
                SELECT TS, NAME, VALUE
                FROM HISTORY
                WHERE NAME IN ('{tag}') AND
                TS BETWEEN CURRENT_TIMESTAMP - {duration} AND CURRENT_TIMESTAMP AND
                PERIOD = '00:0{period}' AND
                REQUEST = {request}
                '''
            else:
                sql_query = f'''
                SELECT TS, NAME, VALUE
                FROM HISTORY
                WHERE NAME IN ('{tag}') AND
                TS BETWEEN CURRENT_TIMESTAMP - {duration} AND CURRENT_TIMESTAMP AND
                REQUEST = {request}
                '''

            if pivot is True:
                df = (pd.read_sql(sql_query, self.conn).pivot_table(index=['TS'], columns=['NAME'], values='VALUE', aggfunc={'VALUE':np.sum}))
                print(f'Tags pulled: {list(df.columns)}', f'\t# of Rows: {len(df.index)}')
            else:
                df = (pd.read_sql(sql_query, self.conn))
            datalist.append(df)
        tagdata = pd.concat(datalist)

        return tagdata.reset_index()

    def ip_analog(self, tag_list=None):
        tag_list = self.parse_tag_list(tag_list)

        sql_query = f'''
        SELECT *
        FROM IP_AnalogDef
        WHERE NAME LIKE '%{tag_list}%'
        '''

        df = pd.read_sql(sql_query, self.conn)

        return df

    def iogethistdef(self, tag_list=None, iogethistdef=''):
        tag_list = self.parse_tag_list(tag_list)

        sql_query = f'''
        SELECT NAME, IO_TAGNAME, "IO_VALUE_RECORD&&FLD"
        FROM IOGetHistDef
        WHERE "IO_VALUE_RECORD&&FLD" LIKE '{tag_list}' AND
        NAME LIKE '%{iogethistdef}%'
        '''

        df = pd.read_sql(sql_query, self.conn)

        return df

    def iostatus(self):
        '''Get the IOGetHistDef table overview of good and bad tags from the different sources
        '''
        sql_query = f'''
        SELECT *
        FROM IOGetHistDef
        '''
        df = pd.read_sql(sql_query, self.conn)

        return df

    def query(self, query):
        df = pd.read_sql(query, self.conn)
        return df

    def parse_tag_list(self, tag_list):
        if tag_list is None:
            tag_list = '%'
        elif type(tag_list) == str:
            #tag_list = f"%{tag_list}%"
            tag_list = f"('{tag_list}')"
        else:
            tag_list = f"{tuple(tag_list)}"

        return tag_list

