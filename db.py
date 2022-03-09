import pymongo

db_connection = pymongo.MongoClient("mongodb://localhost:27017/")
myclient = db_connection['recession_time_historical_data']

def get_ticker_connection(): return myclient['ticker']
def get_historical_data_connection(): return myclient['historical_data']

def update_db(col, data, custom_id_field):
    if "_id" in data: del data['_id']
    try: col.update_one({custom_id_field:data[custom_id_field]}, {"$set":data})
    except Exception as e:
        print(e)
        return

def bulk_insers(col, arr, custom_id_field='id'):
    try: col.insert_many(arr)
    except:
        for a in arr: update_db(col, a, custom_id_field)
