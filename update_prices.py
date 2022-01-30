#!/usr/bin/env python3

import logging
import os
import sys
from datetime import datetime, timedelta
from time import sleep

import keys
import logconfig
import pandas as pd
import sqlalchemy.types as sdt
from binandata import Price
from git import Repo
from loguru import logger as log
from processlock import PLock
from sqlalchemy import create_engine, inspect

loglevel = 'DEBUG'
__progname__ = 'update_prices'
__description__ = "Crypto Price Updater"


def insert_do_nothing_on_conflicts(sqltable, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert
    from sqlalchemy import table, column
    columns = []
    for c in keys:
        columns.append(column(c))
    if sqltable.schema:
        table_name = '{}.{}'.format(sqltable.schema, sqltable.name)
    else:
        table_name = sqltable.name
    mytable = table(table_name, *columns)
    insert_stmt = insert(mytable).values(list(data_iter))
    do_nothing_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['open_time'])
    conn.execute(do_nothing_stmt)


def gitupdatecheck():
    log.debug(f'Checking for updates...')
    localdir = keys.localdir
    repo = Repo(localdir)
    origin = repo.remotes.origin
    origin.fetch()
    if repo.head.commit != origin.refs[0].commit:
        log.info(f'New version found. Updating and Restarting...')
        origin.pull()
        Popen(['restarter.py', '--exec', os.path.dirname(sys.argv[0])])
    else:
        log.debug('No updates found')


def checkerrorlog(record):
    if record["level"] == "WARNING" or record["level"] == "ERROR" or record["level"] == "CRITICAL":
        return True
    else:
        return False


log.remove()
shortlogformat = "<level>{time:YYYY-MM-DD HH:mm:ss.SSS}</level><fg 248>|</fg 248><level>{level: <7}</level><fg 248>|</fg 248> <level>{message}</level>"
longlogformat = "<level>{time:YYYY-MM-DD HH:mm:ss.SSS}</level><fg 248>|</fg 248> <level>{level: <7}</level> <fg 248>|</fg 248> <level>{message: <72}</level> <fg 243>|</fg 243> <fg 109>{name}:{function}:{line}</fg 109>"
log.add(sys.stderr, level=loglevel, format=longlogformat)
log.add(sink='/var/log/investomatic/error.log', level=40, buffering=1, enqueue=True, backtrace=True, diagnose=True, colorize=False, format=longlogformat, delay=False, filter=checkerrorlog)


if loglevel == 'DEBUG':
    logging.basicConfig(level=logging.DEBUG)
elif loglevel == 'INFO':
    logging.basicConfig(level=logging.INFO)
elif loglevel == 'WARNING':
    logging.basicConfig(level=logging.WARNING)


processlock = PLock()
processlock.lock()

gitupdatecheck()

log.info(f'*** {__description__} is starting with log level {loglevel} ***')

coins = []
log.debug(f'Starting sqlalchemy postgresql connection for positions check')
try:
    db_string2 = f'postgresql://{keys.username}:{keys.password}@{keys.hostname}:{keys.port}/personal'
    db2 = create_engine(db_string2)
    log.debug(f'Postgresql database connection established')
except Exception as error:
    log.critical(f'Error connecting to postgresql database: {error}')
    exit(1)

position_table = pd.read_sql_table('positions', db2)
position_table.set_index('symbol', inplace=True)
for index, value in position_table.iterrows():
    coins.append(f'{index.upper()}USDT')

#coins = ['BTCUSDT']
log.debug(f'Found positions: {coins}')

da = Price()

for coin in coins:
    if da.exists(coin) is not None:
        log.debug(f'Starting postgresql database connection for history storage')
        try:
            db_string = f'postgresql://{keys.username}:{keys.password}@{keys.hostname}:{keys.port}/{keys.database}'
            db = create_engine(db_string)
            log.debug(f'Postgresql database connection established')
        except Exception as error:
            log.critical(f'Error connecting to postgresql database: {error}')
            exit(1)

        if not inspect(db).has_table(coin.lower()):
            log.info(f'Table {coin} does not exist in database, creating')
            create_script = f""" CREATE TABLE IF NOT EXISTS {coin.lower()} (
                open_time        TIMESTAMP PRIMARY KEY NOT NULL UNIQUE,
                open             NUMERIC(14, 8) NOT NULL,
                high             NUMERIC(14, 8) NOT NULL,
                low              NUMERIC(14, 8) NOT NULL,
                close            NUMERIC(14, 8) NOT NULL,
                volume           NUMERIC(24, 8) NOT NULL,
                close_time       TIMESTAMP NOT NULL UNIQUE,
                asset_volume     NUMERIC(24, 8) NOT NULL,
                trades           NUMERIC(8, 0) NOT NULL,
                taker_buy_base   NUMERIC(24, 8) NOT NULL,
                taker_buy_quote  NUMERIC(24, 8) NOT NULL
            )"""
            db.execute(create_script)
            log.info(f'Starting data backfill for [{coin}]')

            ds = datetime(2018, 1, 1)
            de = datetime(2022, 1, 22)
            dt = ds
            while dt < de:
                dsstr = ds.strftime('%d %b, %Y')
                dt = ds + timedelta(days=7)
                dtstr = dt.strftime('%d %b, %Y')
                bc = da.price_feeder(coin, historic=True, start=dsstr, stop=dtstr)
                if bc is not None:
                    log.info(f'Backfilling {dsstr} to {dtstr} for [{coin}] with {len(bc.index)} entries')
                    bc.to_sql(coin.lower(), db, if_exists='append', index=True, index_label='open_time', method=insert_do_nothing_on_conflicts)
                ds = dt

        else:
            log.debug(f'Table exists for [{coin}], skipping creation and data backfill')

        log.debug(f'Writing [{coin}] price data to database')

        bc = da.price_feeder(coin)
        bc.to_sql(coin.lower(), db, if_exists='append', index=True, index_label='open_time', method=insert_do_nothing_on_conflicts)
    else:
        log.critical(f'Cannot find [{coin}] on binance exhcange')
        exit(1)
