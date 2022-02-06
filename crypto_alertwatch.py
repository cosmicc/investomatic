#!/usr/bin/env python3

import logging
import sys
from datetime import datetime

import keys
import numpy as np
import pandas as pd
import pytz
from loguru import logger as log
from processlock import PLock
from sqlalchemy import create_engine, inspect
from utils import elapsedTime, estconvert, gitupdatecheck

__loglevel__ = 'DEBUG'
__progname__ = 'crypto_alertwatch'
__description__ = "Crypto Alert Watch"


def checkerrorlog(record):
    if record["level"] == "WARNING" or record["level"] == "ERROR" or record["level"] == "CRITICAL":
        return True
    else:
        return False


log.remove()
log.level("START", no=38, color="<fg 39>", icon="¤")
shortlogformat = "<level>{time:YYYY-MM-DD HH:mm:ss.SSS}</level><fg 248>|</fg 248><level>{level: <7}</level><fg 248>|</fg 248> <level>{message}</level>"
longlogformat = "<level>{time:YYYY-MM-DD HH:mm:ss.SSS}</level><fg 248>|</fg 248> <level>{level: <7}</level> <fg 248>|</fg 248> <level>{message: <72}</level> <fg 243>|</fg 243> <fg 109>{name}:{function}:{line}</fg 109>"
log.add(sys.stderr, level=__loglevel__, format=longlogformat)
log.add(sink='/var/log/investomatic/error.log', level=40, buffering=1, enqueue=True, backtrace=True, diagnose=True, colorize=False, format=longlogformat, delay=False, filter=checkerrorlog)


if __loglevel__ == 'DEBUG':
    logging.basicConfig(level=logging.DEBUG)
elif __loglevel__ == 'INFO':
    logging.basicConfig(level=logging.INFO)
elif __loglevel__ == 'WARNING':
    logging.basicConfig(level=logging.WARNING)
elif __loglevel__ == 'ERROR':
    logging.basicConfig(level=logging.WARNING)

processlock = PLock()
processlock.lock()

gitupdatecheck()

class History_Data:

    def __init__(self):
        log.debug(f'Starting sqlalchemy postgresql connection for history data')
        try:
            db_string = f'postgresql://{keys.username}:{keys.password}@{keys.hostname}:{keys.port}/{keys.database}'
            self.data = create_engine(db_string)
            log.debug(f'Postgresql database connection established')
            self.history_tables = inspect(self.data).get_table_names()
            log.debug(f'Ticker history tables found: {self.history_tables}')
            self.coins = []
            for coin in self.history_tables:
                self.coins.append(coin.replace('usdt', ''))
            log.debug(f'Coins parsed: {self.coins}')
        except Exception as error:
            log.critical(f'Error connecting to postgresql database: {error}')
            exit(1)

    def price(self, ticker):
        if ticker.lower() not in self.coins:
            log.error(f'No history found for [{ticker}] in database')
            return None
        else:
            history_query = f"""SELECT * FROM {ticker.lower()}usdt WHERE close_time BETWEEN NOW() - INTERVAL '15 minutes' and NOW() ORDER BY open_time DESC LIMIT 1;"""
            log.debug(f"""History query: {history_query}""")
            history_table = pd.read_sql_query(history_query, self.data)
            log.debug(f'History query returned {len(history_table)} rows or data')
            if len(history_table) == 0:
                return None
            else:
                close_time = history_table['close_time'][0].replace(tzinfo=pytz.timezone('America/New_York'))
                return {'price': history_table['close'][0], 'timestamp': close_time, 'age': elapsedTime(close_time, datetime.now())}

    def change(self, ticker, interval):
        if ticker.lower() not in self.coins:
            log.error(f'No history found for [{ticker}] in database')
            return None
        else:
            history_query = f"""SELECT * FROM {ticker.lower()}usdt WHERE open_time BETWEEN NOW() - INTERVAL '{interval}' AND NOW() ORDER BY open_time DESC"""
            log.debug(f"""History query: {history_query}""")
            history_table = pd.read_sql_query(history_query, self.data)
            log.debug(f'History query returned {len(history_table)} rows of data')
            if len(history_table) == 0:
                return None
            else:
                stop = history_table['close'][0]
                start = history_table['open'][len(history_table) - 1]
                close_time = history_table['close_time'][0].replace(tzinfo=pytz.timezone('America/New_York'))
                return {'change': round(((stop - start) / start) * 100, 2), 'end_price': history_table['close'][0], 'start_price': history_table['open'][len(history_table) - 1], 'timestamp': close_time, 'age': elapsedTime(close_time, datetime.now()), 'time_range': interval.title()}

    def avg_trades(self, ticker, interval='15 minutes'):
        if ticker.lower() not in self.coins:
            log.error(f'No history found for [{ticker}] in database')
            return None
        else:
            history_query = f"""SELECT * FROM {ticker.lower()}usdt WHERE open_time BETWEEN NOW() - INTERVAL '{interval}' AND NOW() ORDER BY open_time DESC"""
            log.debug(f"""History query: {history_query}""")
            history_table = pd.read_sql_query(history_query, self.data)
            log.debug(f'History query returned {len(history_table)} rows of data')
            if len(history_table) == 0:
                return None
            else:
                close_time = history_table['close_time'][0].replace(tzinfo=pytz.timezone('America/New_York'))
                return {'avg_trades': int(history_table['trades'].mean()), 'timestamp': close_time, 'age': elapsedTime(close_time, datetime.now()), 'time_range': interval.title()}

    def avg_volume(self, ticker, interval='15 minutes'):
        if ticker.lower() not in self.coins:
            log.error(f'No history found for [{ticker}] in database')
            return None
        else:
            history_query = f"""SELECT * FROM {ticker.lower()}usdt WHERE open_time BETWEEN NOW() - INTERVAL '{interval}' AND NOW() ORDER BY open_time DESC"""
            log.debug(f"""History query: {history_query}""")
            history_table = pd.read_sql_query(history_query, self.data)
            log.debug(f'History query returned {len(history_table)} rows of data')
            if len(history_table) == 0:
                return None
            else:
                close_time = history_table['close_time'][0].replace(tzinfo=pytz.timezone('America/New_York'))
                return {'avg_volume': int(history_table['volume'].mean()), 'timestamp': close_time, 'age': elapsedTime(close_time, datetime.now()), 'time_range': interval.title()}


log.log('START', f'{__description__} is starting with log level [{__loglevel__}]')

history = History_Data()

cprice = history.avg_volume('btc', interval='6 months')
print(cprice)

log.info('Script complete.')
