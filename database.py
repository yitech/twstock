import sqlite3
import locale
from locale import atoi
from locale import atof
import pandas as pd
import numpy as np


class StockAPI:
    def __init__(self, src='stock.db'):
        self.src = src
        self.conn = sqlite3.connect(src)
        self.cursor = self.conn.cursor()

    def select_symbol(self, symbol):
        sym = (str(symbol), )
        self.cursor.execute('''SELECT * FROM stocks WHERE 證券代號=?''', sym)
        df = pd.DataFrame(columns=["日期", "證券代號", "證券名稱", "成交股數", "成交筆數", "成交金額", "開盤價",
                                   "最高價", "最低價", "收盤價", "漲跌", "漲跌價差", "最後揭示買價", "最後揭示買量",
                                   "最後揭示賣價", "最後揭示賣量", "本益比"])
        for row in self.cursor.fetchall():
            dict_data = self.tuple_to_dict(row)
            for key in dict_data.keys():
                dict_data[key] = [dict_data[key]]
            pd_data = pd.DataFrame.from_dict(dict_data)
            df = df.append(pd_data)
        df = df.reset_index(drop=True)
        return df

    def select_date(self, date):
        date = (str(date), )
        self.cursor.execute('''SELECT * FROM stocks WHERE 日期=?''', date)
        df = pd.DataFrame(columns=["日期", "證券代號", "證券名稱", "成交股數", "成交筆數", "成交金額", "開盤價",
                                   "最高價", "最低價", "收盤價", "漲跌", "漲跌價差", "最後揭示買價", "最後揭示買量",
                                   "最後揭示賣價", "最後揭示賣量", "本益比"])
        for row in self.cursor.fetchall():
            dict_data = self.tuple_to_dict(row)
            for key in dict_data.keys():
                dict_data[key] = [dict_data[key]]
            pd_data = pd.DataFrame.from_dict(dict_data)
            df = df.append(pd_data)
        df = df.reset_index(drop=True)
        return df

    def create_table(self):
        self.cursor.execute('''CREATE TABLE stocks
                             (日期, 證券代號, 證券名稱, 成交股數, 成交筆數, 成交金額, 開盤價, 最高價, 最低價, 收盤價, 漲跌, 
                             漲跌價差, 最後揭示買價, 最後揭示買量, 最後揭示賣價, 最後揭示賣量, 本益比)''')
        return 0

    def insert_a_dataframe(self, df, date):
        for i, data in df.iterrows():
            self.insert_a_row(data, date)
        return 0

    def commit(self):
        self.conn.commit()
        return 0

    def close(self):
        self.conn.close()
        return 0

    def insert_a_row(self, data, date):
        data = self.structure_protect(data, date)
        data_str = "('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')"\
            .format(data["日期"], data["證券代號"], data["證券名稱"], data["成交股數"], data["成交筆數"], data["成交金額"],
                    data["開盤價"], data["最高價"], data["最低價"], data["收盤價"], data["漲跌"],
                    data["漲跌價差"],data["最後揭示買價"], data["最後揭示買量"], data["最後揭示賣價"],
                    data["最後揭示賣量"], data["本益比"])
        self.cursor.execute("INSERT INTO stocks VALUES {}".format(data_str))
        return 0

    def tuple_to_dict(self, t):
        index = ["日期", "證券代號", "證券名稱", "成交股數", "成交筆數", "成交金額", "開盤價", "最高價", "最低價",
                 "收盤價", "漲跌", "漲跌價差", "最後揭示買價", "最後揭示買量", "最後揭示賣價", "最後揭示賣量", "本益比"]
        dict_data = {}
        for idx, key in enumerate(index):
            dict_data[key] = t[idx]
        self.covertor(dict_data)
        return dict_data

    def structure_protect(self, data, date):
        locale.setlocale(locale.LC_NUMERIC, '')
        dict_data = data.to_dict()
        for key, value in dict_data.items():
            if dict_data[key] == '--':
                dict_data[key] = ''
        dict_data["日期"] = str(date)
        dict_data["證券代號"] = str(data["證券代號"])
        dict_data["證券名稱"] = str(data["證券名稱"])
        if type(data["成交股數"]) == str:
            dict_data["成交股數"] = atoi(data["成交股數"])
        if type(data["成交筆數"]) == str:
            dict_data["成交筆數"] = atoi(data["成交筆數"])
        if type(data["成交金額"]) == str:
            dict_data["成交金額"] = atof(data["成交金額"])
        if type(data["開盤價"]) == str:
            try:
                dict_data["開盤價"] = atof(data["開盤價"])
            except ValueError:
                dict_data["開盤價"] = ''
        if type(data["最高價"]) == str:
            try:
                dict_data["最高價"] = atof(data["最高價"])
            except ValueError:
                dict_data["最高價"] = ''
        if type(data["收盤價"]) == str:
            try:
                dict_data["收盤價"] = atof(data["收盤價"])
            except ValueError:
                dict_data["收盤價"] = ''
        if type(data["最低價"]) == str:
            try:
                dict_data["最低價"] = atof(data["最低價"])
            except ValueError:
                dict_data["最低價"] = ''
        dict_data["漲跌"] = str(data["漲跌"])
        if type(data["漲跌價差"]) == str:
            dict_data["漲跌價差"] = atof(data["漲跌價差"])
        if type(data["最後揭示買價"]) == str:
            try:
                dict_data["最後揭示買價"] = atof(data["最後揭示買價"])
            except ValueError:
                dict_data["最後揭示買價"] = ''
        if type(data["最後揭示買量"]) == str:
            dict_data["最後揭示買量"] = atoi(data["最後揭示買量"])
        if type(data["最後揭示賣價"]) == str:
            try:
                dict_data["最後揭示賣價"] = atof(data["最後揭示賣價"])
            except ValueError:
                dict_data["最後揭示買價"] = ''
        if type(data["最後揭示賣量"]) == str:
            dict_data["最後揭示賣量"] = atof(data["最後揭示賣量"])
        if type(data["本益比"]) == str:
            dict_data["本益比"] = atof(data["本益比"])
        return dict_data

    def covertor(self, data):
        data["日期"] = str(data["日期"])
        data["證券代號"] = str(data["證券代號"])
        data["證券名稱"] = str(data["證券名稱"])
        if type(data["成交股數"]) == str:
            data["成交股數"] = atoi(data["成交股數"])
        if type(data["成交筆數"]) == str:
            data["成交筆數"] = atoi(data["成交筆數"])
        if type(data["成交金額"]) == str:
            data["成交金額"] = atof(data["成交金額"])
        if type(data["開盤價"]) == str:
            try:
                data["開盤價"] = atof(data["開盤價"])
            except ValueError:
                data["開盤價"] = np.nan
        if type(data["最高價"]) == str:
            try:
                data["最高價"] = atof(data["最高價"])
            except ValueError:
                data["最高價"] = np.nan
        if type(data["收盤價"]) == str:
            try:
                data["收盤價"] = atof(data["收盤價"])
            except ValueError:
                data["收盤價"] = np.nan
        if type(data["最低價"]) == str:
            try:
                data["最低價"] = atof(data["最低價"])
            except ValueError:
                data["最低價"] = np.nan
        data["漲跌"] = str(data["漲跌"])
        if type(data["漲跌價差"]) == str:
            data["漲跌價差"] = atof(data["漲跌價差"])
        if type(data["最後揭示買價"]) == str:
            try:
                data["最後揭示買價"] = atof(data["最後揭示買價"])
            except ValueError:
                data["最後揭示買價"] = np.nan
        if type(data["最後揭示買量"]) == str:
            try:
                data["最後揭示買量"] = atoi(data["最後揭示買量"])
            except ValueError:
                data["最後揭示買量"] = np.nan
        if type(data["最後揭示賣價"]) == str:
            try:
                data["最後揭示賣價"] = atof(data["最後揭示賣價"])
            except ValueError:
                data["最後揭示買價"] = np.nan
        if type(data["最後揭示賣量"]) == str:
            try:
                data["最後揭示賣量"] = atof(data["最後揭示賣量"])
            except ValueError:
                data["最後揭示賣量"] = np.nan
        if type(data["本益比"]) == str:
            data["本益比"] = atof(data["本益比"])
        return 0
