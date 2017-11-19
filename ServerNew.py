import requests
from pymongo import MongoClient, errors
from threading import Thread
import _strptime
from datetime import datetime
import time

URL = 'https://poloniex.com/public?command=returnTradeHistory&currencyPair={0}&start={1}&end={2}'


class DataSeeker(object):

    def __init__(self, start_date, end_date, date_format, coin_list):
        self.thread_list = []
        self.coin_list = coin_list[:6]
        self.bits_db = MongoClient("mongodb://benmalka:Bb123456@ds227325.mlab.com:27325/bits")["bits"]
        self.end_epoch = int(time.mktime(time.strptime(end_date, date_format))) + 86400
        self.start_epoch = int(time.mktime(time.strptime(start_date, date_format)))

    def start(self):
        for coin in self.coin_list:
            _t = Thread(target=self.run_collection, args=(coin,))
            self.thread_list.append(_t)
            _t.start()
        for _t in self.thread_list:
            _t.join()

    def run_date(self, date, coin, count, no_good_dates, collection):
        s_epoch = date
        e_epoch = date + 86399
        ans = requests.get(URL.format(coin, s_epoch, e_epoch))
        try:
            ans = ans.json()
        except Exception as e:
            print ans, datetime.fromtimestamp(date).strftime('%Y-%m-%d')
            no_good_dates.append(date)
            return
        print count, datetime.fromtimestamp(date).strftime('%Y-%m-%d'), coin
        results = {"end_price": float(ans[0]["rate"]), "start_price": float(ans[-1]["rate"]), "b_value": 0,
                   "s_value": 0,
                   "percentage": 100 * ((float(ans[0]["rate"]) - float(ans[-1]["rate"])) / float(ans[-1]["rate"])),
                   "date": datetime.fromtimestamp(date)}
        for s in ans:
            if s["type"] == "buy":
                results["b_value"] += float(s['amount'])
            else:
                results["s_value"] += float(s['amount'])
        collection.insert_one(results)

    def run_collection(self, coin):
        try:
            self.bits_db.create_collection(coin)
        except errors.CollectionInvalid:
            pass
        collection = self.bits_db.get_collection(coin)
        count = 0
        no_good_dates = []
        date = self.start_epoch
        while date < self.end_epoch:
            count += 1
            self.run_date(date, coin, count, no_good_dates, collection)
            date += 86400
            time.sleep(1)
        for _date in no_good_dates:
            count += 1
            self.run_date(_date, coin, count, [], collection)

a = DataSeeker("01-10-2015", "31-10-2017", "%d-%m-%Y", ["BTC_VRC"])
a.start()
