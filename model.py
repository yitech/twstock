import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.arima_model import ARIMA
import talib
import copy
import datetime
import numpy as np
import database as db


class Model:
    def __init__(self, src='stock.db'):
        self.db = db.StockAPI(src)
        self.symbol = []
        self.dataframe = {}
        self.model = {}
        self.strategy = []

    def summarize_model(self, symbol):
        sym_model = self.model[str(symbol)]
        sym_model.summary()
        return 0

    def extract_dataframe(self):
        for sym in self.symbol:
            df = TWStockDataFrame(self.db.select_symbol(sym))
            df.convert_date()
            df.fill_missing()
            df.eval_log_return("收盤價")
            self.dataframe.update({sym: df})
        return 0

    def build_holt_model(self):
        USE_DATA = 250
        for sym in self.symbol:
            data_set = self.dataframe[sym]
            close_return = data_set["收盤價報酬率"].values
            model = ExponentialSmoothing(close_return[-USE_DATA:])
            model_fit = model.fit()
            self.model.update({sym: model_fit})
        return 0

    def build_ARIMA_model(self):
        USE_DATA = 250
        for sym in self.symbol:
            data_set = TWStockDataFrame(self.db.select_symbol(sym))
            data_set.eval_log_return("收盤價")
            data_set.fill_missing()
            close_return = data_set["收盤價報酬率"].values
            model = ARIMA(close_return[-USE_DATA:], order=(1, 1, 1))
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

    def convert_date(self):
        self.index = pd.to_datetime(self["日期"])
        self.__init__(self.drop(columns=["日期"]))
        return 0

    def eval_log_return(self, key):
        postfix = "報酬率"
        log_return = [np.nan]
        for yesterday, today in zip(self.index[:-1], self.index[1:]):
            try:
                log_return.append(np.log(self.loc[today][key] / self.loc[yesterday][key]))
            except TypeError:
                log_return.append(0)
        df_log_return = pd.DataFrame(data=log_return, columns=[key + postfix], index=self.index)
        self[key + postfix] = df_log_return
        return 0

    def fill_missing(self):
        key = ["開盤價", "收盤價", "最高價", "最低價"]
        for yesterday, today in zip(self.index[:-1], self.index[1:]):
            for k in key:
                if self.loc[today][k] == np.nan:
                    self.loc[today][k] = self.loc[yesterday]["收盤價"]
                    print(self.loc[today][k])
        return 0


class TimeSeriesAnalysis:
    def __init__(self, df):
        self.period = 250
        self.df = TWStockDataFrame(df)
        self.df.convert_date()
        self.keys = ["開盤價", "收盤價", "最高價", "最低價"]
        self.eval_log_return()
        self.model = {}

    def build_ARIMA_model(self):
        sample = TWStockDataFrame(self.df.iloc[-self.period:])
        postfix = "報酬率"
        for k in self.keys:
            lgrt = sample[k + postfix]
            model = ARIMA(lgrt.values, order=(3, 1, 1))
            model_fit = model.fit(disp=False, method='mle')
            self.model.update({k+postfix: model_fit})
        return 0

    def view_return_predict(self):
        plt.subplot(221)
        self.plot_return_predict("開盤價")
        plt.subplot(222)
        self.plot_return_predict("最高價")
        plt.subplot(223)
        self.plot_return_predict("最低價")
        plt.subplot(224)
        self.plot_return_predict("收盤價")
        plt.show()
        return 0

    def plot_return_predict(self, key):
        postfix = "報酬率"
        translate = {"開盤價": "open", "收盤價": "close", "最高價": "max", "最低價": " min"}
        sample = TWStockDataFrame(self.df.iloc[-self.period:])
        model_result = self.model[key + postfix]
        r_hat = model_result.predict(1, self.period, sample[key + postfix].values)
        t = sample.index
        plt.plot(t, sample[key + postfix].values, label=translate[key])
        plt.plot(t, -r_hat, label=translate[key] + "_hat")
        plt.legend()
        return 0

    def plot_stock_price(self, key):
        return 0

    def eval_log_return(self):
        for k in self.keys:
            self.df.eval_log_return(k)
        return 0



if __name__ == '__main__':
    #md = Model()
    #md.intersection_selection(3000, 7)
    #md.build_holt_model()

    api = db.StockAPI()
    tsa = TimeSeriesAnalysis(api.select_symbol(2330))#
    tsa.build_ARIMA_model()
    tsa.view_return_predict()




