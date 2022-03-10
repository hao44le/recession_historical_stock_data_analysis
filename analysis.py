from db import *
import pandas as pd
from datetime import datetime

historical_data_connection = get_historical_data_connection()
ticker_connection = get_ticker_connection()
data = historical_data_connection.find({})

results = {}
for r in data: 
    ticker = r['ticker']
    if ticker in results: results[ticker].append([{'date':r['date'],'close':r['close']}])
    else: results[ticker] = [{'date':r['date'],'close':r['close']}]

final_results = []
for ti in results:
    ti_record = ticker_connection.find_one({"Symbol":ti})

    smallest_datetime = results[ti][0]
    largest_datetime = None
    for tmp in results[ti][1:]:
        to_dt = datetime(1981, 1, 1)
        if tmp[0]['date'] > to_dt:
            largest_datetime = tmp[0]
            break
    start_price = smallest_datetime['close']
    # print(largest_datetime)
    end_price = largest_datetime['close']
    diff = end_price - start_price
    percent_gain = diff / start_price
    final_results.append([ti, smallest_datetime['date'], largest_datetime['date'], start_price, end_price, diff, percent_gain, ti_record['Name'], ti_record['Market Cap'], ti_record['Country'], ti_record["Sector"], ti_record['Industry']])
    # break
# print(final_results)
data = pd.DataFrame(final_results, columns = ['Ticker', 'start_date','end_date','start_price','end_price','price_diff','diff percentage', 'Name', 'Market Cap', 'Country', 'Sector', 'Industry'])
data = data.sort_values(by=['diff percentage'], ascending=False)
data.to_csv("result.csv")