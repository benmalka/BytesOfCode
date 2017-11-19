import requests
from pymongo import MongoClient
from threading import Thread
import _strptime
from datetime import datetime
import time

bits_db = MongoClient("mongodb://benmalka:Bb123456@ds227325.mlab.com:27325/bits")["bits"]
URL = 'https://poloniex.com/public?command=returnTradeHistory&currencyPair={0}&start={1}&end={2}'
COINS = ['USDT_BTC', 'BTC_DASH', 'BTV_LTC', 'BTC_XRP', 'BTC_STR', 'BTC_XMR', 'BTC_XVC', 'BTC_NXT']
_COINS = ['BTC_XVC', 'BTC_NXT']
dates = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
_threads = []


def run_date(date, coin, count, no_good_dates, collection):
    s_date_time = '{} 00:00:00'
    e_date_time = '{} 23:59:59'
    pattern = '%Y-%m-%d %H:%M:%S'
    s_epoch = int(time.mktime(time.strptime(s_date_time.format(date), pattern)))
    e_epcoh = int(time.mktime(time.strptime(e_date_time.format(date), pattern)))
    ans = requests.get(URL.format(coin, s_epoch, e_epcoh))
    try:
        ans = ans.json()
    except Exception as e:
        print ans, date
        no_good_dates.append(date)
        return
    print count, date, coin
    results = {"end_price": float(ans[0]["rate"]), "start_price": float(ans[-1]["rate"]), "b_value": 0, "s_value": 0,
               "percentage": 100 * ((float(ans[0]["rate"]) - float(ans[-1]["rate"])) / float(ans[-1]["rate"])),
               "date": datetime.strptime(date, "%Y-%m-%d")}
    for s in ans:
        if s["type"] == "buy":
            results["b_value"] += float(s['amount'])
        else:
            results["s_value"] += float(s['amount'])
    collection.insert_one(results)


def run_collection(start_month, start_year, coin):
    bits_db.create_collection(coin)
    collection = bits_db.get_collection(coin)
    _year = start_year
    count = 0
    no_good_dates = []
    for num in xrange(0, 24):
        month = num + start_month
        _month = 12 if month % 12 == 0 else month % 12
        if _month == 1:
            _year += 1
        for day in xrange(1, dates[_month-1]+1):
            _day = str(day) if day >= 10 else "0{}".format(day)
            _month = str(_month) if _month >= 10 else "0{}".format(_month)
            current_date = "20{}-{}-{}".format(_year, _month, _day)
            count += 1
            run_date(current_date, coin, count, no_good_dates, collection)
            time.sleep(1)
    for _date in no_good_dates:
        count += 1
        run_date(_date, coin, count, [], collection)


for C in _COINS:
    _t = Thread(target=run_collection, args=(11, 15, C))
    _threads.append(_t)
    _t.start()

for _t in _threads:
    _t.join()
