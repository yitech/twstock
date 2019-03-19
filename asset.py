import datetime
import copy
import os
from enum import IntEnum
import pandas as pd


class Position(IntEnum):
    Long = 1
    Short = 0


class Asset:
    def __init__(self, ntd, src=os.path.join("data", "daily")):
        self.ntd = ntd
        self.possess ={}
        self.src = src

    def summary(self):
        print("NTD: {}".format(self.ntd))
        for key, lot in self.possess.items():
            print("{}: {}".format(key, int(lot)))
        return 0

    def eval_value(self, at=datetime.datetime.today().strftime("%Y%m%d")):
        value = 0
        a_day = datetime.timedelta(days=1)
        date = datetime.datetime.strptime(at, "%Y%m%d")
        isTrade = False
        while not isTrade:
            date_str = date.strftime("%Y%m%d")
            file_src = os.path.join(self.src, date_str + ".csv")
            try:
                df = pd.read_csv(file_src)
                for code, lot in self.possess.items():
                    stock = df[df["證券代號"] == str(code)]
                    price = float(stock["收盤價"].values[0])
                    value += price * 1000 * lot
                isTrade = True
            except FileNotFoundError:
                date = date - a_day
        value += self.ntd
        return value


class Ticket:
    def __init__(self, date, security, lot, position):
        self.date = date
        self.security = security
        self.lot = lot
        self.position = position


class Market:
    def __init__(self, src=os.path.join("data", "daily")):
        self.src = src
        self.in_fee = 0.001425
        self.out_fee = 0.001425
        self.tax = 0.003

    def asset_change(self, asset, ls_tickets):
        capital = copy.deepcopy(asset)
        for t in ls_tickets:
            file_src = os.path.join(self.src, t.date + ".csv")
            df = pd.read_csv(file_src)
            stock = df[df["證券代號"] == str(t.security)]
            max_price = stock["最高價"].values[0]
            min_price = stock["最低價"].values[0]
            price = (float(max_price) + float(min_price)) / 2
            if t.position == Position.Long:
                try:
                    capital.possess[str(t.security)] += t.lot
                except KeyError:
                    capital.possess.update({str(t.security): t.lot})
                capital.ntd -= (1 + self.in_fee) * price * 1000 * t.lot
            else:
                capital.possess[str(t.security)] -= t.lot
                capital.ntd += (1 - self.out_fee - self.tax) * price * 1000 * t.lot
        return capital


if __name__ == '__main__':
    asset = Asset(1000000)
    market = Market()
    ls_ticket = [Ticket(str(20190314), str(2330), 1, Position.Long)]
    asset_ = market.asset_change(asset, ls_ticket)
    asset_.summary()
    print(asset_.eval_value())
