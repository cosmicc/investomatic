from binance.client import Client
import keys
from pandas import DataFrame as df
from data import Price
from time import sleep
from loguru import logger as log
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

client = Client(api_key=keys.api_key, api_secret=keys.api_secret)

# print(client.ping())  # returns empty dict
# print(client.get_server_time())  # epoch server time
# print(client.get_system_status())  # 0 normal or 1 System Maintenance
# print(client.get_symbol_info('BTCUSDT'))


info = client.get_account()
print(info)
balances = info['balances']
print(balances)


exit(0)
#ap = prices.latest_data('BTCUSDT')
#ap = prices.get_price('BTCUSDT')

# print(ap)
# print(type(ap))

"""
def wavg(group, avg_name, weight_name):
    http://stackoverflow.com/questions/10951341/pandas-dataframe-aggregate-function-using-multiple-columns
    In rare instance, we may not have weights, so just return the mean. Customize this if your business case
    should return otherwise.
    d = np.asarray(group[avg_name])
    w = np.asarray(group[weight_name])
    print(d)
    print(w)
    try:
        return (d * w).sum() / w.sum()
    except ZeroDivisionError:
        return d.mean()


log.debug(f'Starting sqlalchemy postgresql connection')
try:
    db_string = f'postgresql://{keys.username}:{keys.password}@{keys.hostname}:{keys.port}/personal'
    db = create_engine(db_string)
    log.debug(f'Sqlalchemy postgresql connection established')
except Exception as error:
    log.critical(f'Error connecting to postgresql database: {error}')
    exit(1)

log.debug(f'Loading table data from database')
history_table = pd.read_sql_table('history', db)

#t = history_table.groupby("security").apply(lambda x: np.average(x['price'], weights=x['quantity']))
#t = history_table.groupby("security").apply(wavg, "price", "quantity")
# print(t)
"""
