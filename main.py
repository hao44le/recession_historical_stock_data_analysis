import yfinance as yf
from db import *

ticker_connection = get_ticker_connection()
historical_data_connection = get_historical_data_connection()

def download_historical_data(ticker, historical_data_connection):
    data = yf.download(ticker, start="1972-01-01", end="1982-12-30")
    if data.shape[0] == 0: return
    results = []
    for index, row in data.iterrows():
        results.append({'ticker':ticker, 
        'open':row['Open'],
        'high':row['High'],
        'low':row['Low'],
        'close':row['Close'],
        'date':index})
    print(len(results))
    bulk_insers(historical_data_connection,results)

task = ticker_connection.find_one({"status":"0"})
while task is not None:
    symbol = task['Symbol']
    print(symbol)
    ticker_connection.update_one({'Symbol':symbol},{"$set":{"status":1}})
    download_historical_data(symbol, historical_data_connection)
    ticker_connection.update_one({'Symbol':symbol},{"$set":{"status":2}})
    task = ticker_connection.find_one({"status":"0"})
    # break