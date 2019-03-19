import requests
import pandas as pd
import os
import time
from numpy import random
import datetime
from io import StringIO
import sqlite3


class Crawler:
    def __init__(self, root=os.path.join('data', "daily")):
        self.root = root
        if not os.path.isdir(self.root):
            os.mkdir(self.root)

    def build_database(self, start_from, end_up=datetime.datetime.today()):
        a_day = datetime.timedelta(days=1)
        date = start_from
        start_from_str = start_from.strftime("%Y%m%d")
        end_up_str = end_up.strftime("%Y%m%d")
        date_str = date.strftime("%Y%m%d")
        while date_str != end_up_str:
            file_str = os.path.join(self.root, date_str + ".csv")
            if not os.path.isfile(file_str):
                r = requests.post(
                    'http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + date_str + '&type=ALL')
                try:
                    df = pd.read_csv(StringIO("\n".join([i.translate({ord(c): None for c in ' '})
                                                         for i in r.text.split('\n')
                                                         if len(i.split('",')) == 17 and i[0] != '='])), header=0)
                    df = df.drop(columns=["Unnamed: 16"])
                    df.to_csv(file_str, index=False)
                    print("{} is successfully clone!".format(date_str))
                    time.sleep(5.5 + random.randn())
                except:
                    print("{} is empty!".format(date_str))

            else:
                print("{} is exist!".format(file_str))
            date += a_day
            date_str = date.strftime("%Y%m%d")

        print("successfully clone data from {} to {}".format(start_from_str, end_up_str))
        return 0


class Sorter:
    def __init__(self, src=os.path.join("data", "daily"), dst=os.path.join("data", "stock")):
        self.src = src
        self.dst = dst
        if not os.path.isdir(self.dst):
            os.mkdir(self.dst)

    def update_all(self, date_str):
        df = pd.read_csv(os.path.join(self.src, date_str + ".csv"))
        code = df["證券代號"].values
        for c in code:
            self.update_security(c)
        return 0

    def update_security(self, code):
        file_src = os.path.join(self.dst, str(code) + ".csv")
        if not os.path.isfile(file_src):
            self.create_security(code)
        df = pd.read_csv(file_src)
        if df.empty:
            for root, dirs, files in os.walk(self.src):
                files = sorted(files)
                for f in files:
                    date_src = os.path.join(root, f)
                    date_str = f.split(".")[0]
                    df_src = pd.read_csv(date_src)
                    information = df_src[df_src["證券代號"] == str(code)]
                    if information.empty:
                        continue
                    else:
                        information.insert(loc=0, column="日期", value=date_str)
                        df = pd.concat([df, information], axis=0)
            print("{} is updated!".format(code))
            df.to_csv(file_src, index=False)
        else:
            today = datetime.datetime.today()
            a_day = datetime.timedelta(days=1)
            last_day_str = str(df["日期"].iloc[-1])
            start_date = datetime.datetime.strptime(last_day_str, "%Y%m%d")
            date_str = (start_date + a_day).strftime("%Y%m%d")
            end_str = (today + a_day).strftime("%Y%m%d")
            while date_str != end_str:
                try:
                    date_src = os.path.join(self.src, date_str + ".csv")
                    df_src = pd.read_csv(date_src)
                    information = df_src[df_src["證券代號"] == str(code)]
                    if information.empty:
                        continue
                    else:
                        information.insert(loc=0, column="日期", value=date_str)
                        df = pd.concat([df, information], axis=0)
                        print(df)
                except:
                    pass
                day = datetime.datetime.strptime(date_str, "%Y%m%d")
                day = day + a_day
                date_str = day.strftime("%Y%m%d")
            print("{} is updated!".format(code))
            df.to_csv(file_src, index=False)
        return 0


def main():
    start = datetime.datetime(2019, 3, 19)
    crawler = Crawler()
    crawler.build_database(start)
    # sorter = Sorter()
    # sorter.update_all("20190318")


if __name__ == "__main__":
    main()
