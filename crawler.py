import requests
import pandas as pd
import os
import time
from numpy import random
import datetime
from io import StringIO


class Crawler:
    def __init__(self, root='data'):
        self.root = root
        if not os.path.isdir(self.root):
            os.makedirs(self.root)

    def build_database(self, start_from, end_up=datetime.datetime.today().strftime("%Y%m%d")):
        a_day = datetime.timedelta(days=1)
        start_from = datetime.datetime.strptime(start_from, "%Y%m%d")
        start_from_str = start_from.strftime("%Y%m%d")
        end_up = datetime.datetime.strptime(end_up, "%Y%m%d")
        end_up_str = (end_up + a_day).strftime("%Y%m%d")
        date_str = start_from.strftime("%Y%m%d")
        date = start_from
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
                    df = df.rename(columns={"漲跌(+/-)": "漲跌"})
                    df.to_csv(file_str, index=False)
                    print("{} is successfully clone!".format(date_str))
                    time.sleep(5.0 + random.randn())
                except:
                    print("{} is empty!".format(date_str))
                    time.sleep(abs(1.0 + random.randn()))

            else:
                print("{} is exist!".format(file_str))
            date += a_day
            date_str = date.strftime("%Y%m%d")

        print("successfully clone data from {} to {}".format(start_from_str, end_up.strftime("%Y%m%d")))
        return 0


def main():
    crawler = Crawler()
    crawler.build_database('20190324', '20190328')


if __name__ == "__main__":
    main()
