import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.vector_ar.var_model import VAR
import os
import copy
import datetime
import locale
from locale import atoi
import numpy as np
import database as db
import sqlite3


class Model:
    def __init__(self, src='stock.db'):
        self.db = db.StockAPI(src)
        self.symbol = []
        self.model = {}
        self.strategy = []

    def summarize_model(self, symbol):
        sym_model = self.model[str(symbol)]
        sym_model.summary()
        return 0

    def build_model(self):
        USE_DATA = 250
        for sym in self.symbol:
            data_set = TWStockDataFrame(self.db.select_symbol(sym))
            print(data_set)
            data_set.eval_log_return()
            sample = data_set.iloc[-USE_DATA:]
            open_return = sample["開盤價報酬率"].values
            high_return = sample["最高價報酬率"].values
            low_return = sample["最低價報酬率"].values
            close_return = sample["收盤價報酬率"].values
            data = []
            for i in range(0, USE_DATA):
                row = [open_return[i], high_return[i], low_return[i], close_return[i]]
                data.append(row)
            model = VAR(data)
            model_fit = model.fit()
            self.model.update({sym: model_fit})
        return 0

    def intersection_selection(self, volume, days=7, end_day=datetime.datetime.today().strftime("%Y%m%d")):
        a_day = datetime.timedelta(days=1)
        end_day = datetime.datetime.strptime(end_day, "%Y%m%d")
        date = copy.deepcopy(end_day)
        selected = set(self.stock_day_selection(volume, date.strftime("%Y%m%d")))
        for i in range(0, days):
            date = date - a_day
            date_str = date.strftime("%Y%m%d")
            selected = selected.intersection(set(self.stock_day_selection(volume, date_str)))
        self.symbol = list(selected)
        return 0

    def stock_day_selection(self, volume, at):
        # hand selection
        at = str(at)
        a_day = datetime.timedelta(days=1)
        date = datetime.datetime.strptime(at, "%Y%m%d")
        is_trade = False
        while not is_trade:
            date_str = date.strftime("%Y%m%d")
            df = self.db.select_date(date_str)
            if df.empty is True:
                date = date - a_day
            else:
                df = df[df["成交筆數"] > volume]
                is_trade = True
        return df["證券代號"].values


class TWStockDataFrame(pd.DataFrame):
    def __init__(self, df):
        super(TWStockDataFrame, self).__init__(df)

    def eval_log_return(self):
        price_key= ["開盤價", "最高價", "最低價", "收盤價"]
        postfix = "報酬率"
        n = len(self)
        for key in price_key:
            log_return = [np.nan]
            for row in range(1, n):
                try:
                    log_return.append(np.log(self[key][row] / self[key][row - 1]))
                except TypeError:
                    log_return.append(0)
            df_log_return = pd.DataFrame(data=log_return, columns=[key + postfix])
            self[key + postfix] = df_log_return
        return self.loc[0]

    def eval_average(self):
        price_key = ["最高價", "最低價"]



if __name__ == '__main__':
    md = Model()
    md.intersection_selection(3000)
    md.build_model()