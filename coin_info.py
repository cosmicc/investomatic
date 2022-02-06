#!/usr/bin/env python3

import json
from datetime import datetime, timedelta

import keys
import pytz
from cmbdata import CoinMarketBase
from loguru import logger as log
from sqlalchemy import create_engine, inspect
import pandas as pd


class CoinInfo:
    def __init__(self):
        log.debug(f'Starting sqlalchemy postgresql connection')
        self.db = None
        self.coininfo_table = 'coin_info'
        self.top200_table = 'top_200_info'
        self.coinbase = CoinMarketBase()
        self.positions = []
        self.preload_tables = ['coin_info', 'positions', 'top_200_info']
        try:
            db_string = f'postgresql://{keys.username}:{keys.password}@{keys.hostname}:{keys.port}/personal'
            self.db = create_engine(db_string)
            log.debug(f'Sqlalchemy postgresql connection established')
        except Exception as error:
            log.critical(f'Error connecting to postgresql database: {error}')
            exit(1)
        log.debug(f'Preloading tables into cache...')
        for table in self.preload_tables:
            preload_query = f"""SELECT pg_prewarm('{table}')"""
            self.db.execute(preload_query)
        log.debug('Preloading tables into cache complete')

    def update_coin_info(self, ticker=None):
        log.debug(f'Checking for table [{self.coininfo_table}] and creating if does not exist')
        create_script = f""" CREATE TABLE IF NOT EXISTS {self.coininfo_table.lower()} (
            symbol           VARCHAR(10) PRIMARY KEY NOT NULL UNIQUE,
            name             VARCHAR(20) NOT NULL,
            date_launched    TIMESTAMP,
            date_added       TIMESTAMP,
            platform         VARCHAR,
            category         VARCHAR(20),
            slug             VARCHAR(20),
            id               INT,
            notice           VARCHAR,
            logo_url         VARCHAR,
            web_url          VARCHAR,
            explorer_urls    VARCHAR,
            announcement_url VARCHAR,
            tech_doc         VARCHAR,
            subreddit        VARCHAR(20),
            twitter          VARCHAR(20),
            contracts        VARCHAR,
            self_tags        VARCHAR,
            tags             VARCHAR,
            description      VARCHAR,
            last_update      TIMESTAMP
        )"""
        self.db.execute(create_script)

        log.info("Checking positions for missing coin info")
        coininfo_query = f"""SELECT symbol, last_update FROM {self.coininfo_table}"""
        positions_query = f"""SELECT symbol FROM positions"""
        p = self.db.execute(positions_query)
        c = self.db.execute(coininfo_query)
        coininfo = []
        self.positions = []

        for row in p:
            self.positions.append(row[0])

        if ticker is not None:
            self.positions.append(ticker.upper())

        for row in c:
            coininfo.append(row[0])

        for coin in self.positions:
            if coin not in coininfo:
                log.info(f'Inserting new cryptocurrency info into [{self.coininfo_table}] table: [{coin}]')
                insert_string = """INSERT INTO coin_info (symbol, name, date_launched, date_added, platform, category, slug, id, notice, logo_url, web_url, explorer_urls, announcement_url, tech_doc, subreddit, twitter, contracts, self_tags, tags, description, last_update)
                                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

                cinfo = self.coinbase.get_info(coin)

                if type(cinfo['data'][coin.upper()]['urls']['website']) == list:
                    if len(cinfo['data'][coin.upper()]['urls']['website']) == 1:
                        website = cinfo['data'][coin.upper()]['urls']['website'][0]
                    else:
                        website = cinfo['data'][coin.upper()]['urls']['website']
                else:
                    website = cinfo['data'][coin.upper()]['urls']['website']

                if type(cinfo['data'][coin.upper()]['urls']['explorer']) == list:
                    if len(cinfo['data'][coin.upper()]['urls']['explorer']) == 1:
                        explorer = cinfo['data'][coin.upper()]['urls']['explorer'][0]
                    else:
                        explorer = cinfo['data'][coin.upper()]['urls']['explorer']
                else:
                    explorer = cinfo['data'][coin.upper()]['urls']['explorer']

                if type(cinfo['data'][coin.upper()]['urls']['technical_doc']) == list:
                    if len(cinfo['data'][coin.upper()]['urls']['technical_doc']) == 1:
                        tech_doc = cinfo['data'][coin.upper()]['urls']['technical_doc'][0]
                    else:
                        tech_doc = cinfo['data'][coin.upper()]['urls']['technical_doc']
                else:
                    tech_doc = cinfo['data'][coin.upper()]['urls']['technical_doc']

                contracts = json.dumps(cinfo['data'][coin.upper()]['contract_address'])
                platform = json.dumps(cinfo['data'][coin.upper()]['platform'])

                values = (cinfo['data'][coin.upper()]['symbol'], cinfo['data'][coin.upper()]['name'], cinfo['data'][coin.upper()]['date_launched'], cinfo['data'][coin.upper()]['date_added'], platform, cinfo['data'][coin.upper()]['category'], cinfo['data'][coin.upper()]['slug'], cinfo['data'][coin.upper()]['id'],
                          cinfo['data'][coin.upper()]['notice'], cinfo['data'][coin.upper()]['logo'], website, explorer, cinfo['data'][coin.upper()]['urls']['announcement'], tech_doc, cinfo['data'][coin.upper()]['subreddit'],
                          cinfo['data'][coin.upper()]['twitter_username'], contracts, cinfo['data'][coin.upper()]['self_reported_tags'], cinfo['data'][coin.upper()]['tags'], cinfo['data'][coin.upper()]['description'], datetime.now())
                self.db.execute(insert_string, values)
            else:
                c = self.db.execute(coininfo_query)
                for row in c:
                    if row[0] == coin:
                        if row[1] < datetime.now() - timedelta(days=30):
                            log.info(f'Updating old cryptocurrency info into [{self.coininfo_table}] table: [{coin}]')
                            update_string = f"""UPDATE coin_info SET name = %s, date_launched = %s, date_added = %s, platform = %s, category = %s, slug = %s, id = %s, notice = %s, logo_url = %s, web_url = %s,
                                                explorer_urls = %s, announcement_url = %s, tech_doc = %s, subreddit = %s, twitter = %s, contracts = %s, self_tags = %s, tags = %s, description = %s, last_update = %s
                                                WHERE symbol = %s"""

                            cinfo = self.coinbase.get_info(coin)

                            if type(cinfo['data'][coin.upper()]['urls']['website']) == list:
                                if len(cinfo['data'][coin.upper()]['urls']['website']) == 1:
                                    website = cinfo['data'][coin.upper()]['urls']['website'][0]
                                else:
                                    website = cinfo['data'][coin.upper()]['urls']['website']
                            else:
                                website = cinfo['data'][coin.upper()]['urls']['website']

                            if type(cinfo['data'][coin.upper()]['urls']['explorer']) == list:
                                if len(cinfo['data'][coin.upper()]['urls']['explorer']) == 1:
                                    explorer = cinfo['data'][coin.upper()]['urls']['explorer'][0]
                                else:
                                    explorer = cinfo['data'][coin.upper()]['urls']['explorer']
                            else:
                                explorer = cinfo['data'][coin.upper()]['urls']['explorer']

                            if type(cinfo['data'][coin.upper()]['urls']['technical_doc']) == list:
                                if len(cinfo['data'][coin.upper()]['urls']['technical_doc']) == 1:
                                    tech_doc = cinfo['data'][coin.upper()]['urls']['technical_doc'][0]
                                else:
                                    tech_doc = cinfo['data'][coin.upper()]['urls']['technical_doc']
                            else:
                                tech_doc = cinfo['data'][coin.upper()]['urls']['technical_doc']

                            contracts = json.dumps(cinfo['data'][coin.upper()]['contract_address'])
                            platform = json.dumps(cinfo['data'][coin.upper()]['platform'])

                            values = (cinfo['data'][coin.upper()]['name'], cinfo['data'][coin.upper()]['date_launched'], cinfo['data'][coin.upper()]['date_added'], platform, cinfo['data'][coin.upper()]['category'], cinfo['data'][coin.upper()]['slug'], cinfo['data'][coin.upper()]['id'],
                                      cinfo['data'][coin.upper()]['notice'], cinfo['data'][coin.upper()]['logo'], website, explorer, cinfo['data'][coin.upper()]['urls']['announcement'], tech_doc, cinfo['data'][coin.upper()]['subreddit'],
                                      cinfo['data'][coin.upper()]['twitter_username'], contracts, cinfo['data'][coin.upper()]['self_reported_tags'], cinfo['data'][coin.upper()]['tags'], cinfo['data'][coin.upper()]['description'], datetime.now(), coin)
                            self.db.execute(update_string, values)

    def update_latest_info(self):
        insert_string = f"""INSERT INTO {self.top200_table} (symbol, name, last_updated, cmc_rank, slug, id, circulating_supply, max_supply, market_pairs, total_supply, fully_diluted_market_cap,
                           market_cap, market_cap_dominance, price, percent_change_1h, percent_change_24h, percent_change_7d, percent_change_30d, percent_change_60d, percent_change_90d, volume_24h, volume_change_24h)
                           values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        ccinfo = self.coinbase.get_latest_listings()

        if inspect(self.db).has_table(self.top200_table):
            log.info(f'Checking last update time in [{self.top200_table}] table')
            date_query = f"""SELECT last_updated FROM {self.top200_table}"""
            last_info_update = self.db.execute(date_query)
            for f in last_info_update:
                if f[0] > (datetime.now() - timedelta(days=1)):
                    log.info(f'Data less then 24 hours old. Skipping online refresh')
                    return True
            log.info(f'Data older then 24 hours, refreshing data')
            self.db.execute(f"""DROP TABLE {self.top200_table}""")
        else:
            log.warning(f'Table [{self.top200_table}] does not exist, creating inital table')

        create_script = f""" CREATE TABLE {self.top200_table.lower()} (
            symbol                      VARCHAR(10) PRIMARY KEY NOT NULL UNIQUE,
            name                        VARCHAR(30) NOT NULL,
            last_updated                TIMESTAMP,
            cmc_rank                    INT,
            slug                        VARCHAR(30),
            id                          INT,
            circulating_supply          NUMERIC(25,8),
            max_supply                  NUMERIC(25,8),
            market_pairs                INT,
            total_supply                NUMERIC(25,8),
            fully_diluted_market_cap    NUMERIC(25,8),
            market_cap                  NUMERIC(25,8),
            market_cap_dominance        NUMERIC(25,8),
            price                       NUMERIC(25,8),
            percent_change_1h           NUMERIC(25,8),
            percent_change_24h          NUMERIC(25,8),
            percent_change_7d           NUMERIC(25,8),
            percent_change_30d          NUMERIC(25,8),
            percent_change_60d          NUMERIC(25,8),
            percent_change_90d          NUMERIC(25,8),
            volume_24h                  NUMERIC(25,8),
            volume_change_24h           NUMERIC(25,8)
        )"""
        self.db.execute(create_script)

        for cinfo in ccinfo['data']:
            log.info(f'Updating latest info for coin {cinfo["name"]} ({cinfo["symbol"]})')

            dd = datetime.strptime(cinfo['last_updated'], '%Y-%m-%dT%H:%M:%S.%fZ')
            last_update = (dd - timedelta(hours=5)).replace(tzinfo=pytz.timezone('America/New_York'))

            values = (cinfo['symbol'], cinfo['name'], last_update, cinfo['cmc_rank'], cinfo['slug'], cinfo['id'],
                      cinfo['circulating_supply'], cinfo['max_supply'], cinfo['num_market_pairs'], cinfo['total_supply'],
                      cinfo['quote']['USD']['fully_diluted_market_cap'], cinfo['quote']['USD']['market_cap'], cinfo['quote']['USD']['market_cap_dominance'],
                      cinfo['quote']['USD']['price'], cinfo['quote']['USD']['percent_change_1h'], cinfo['quote']['USD']['percent_change_24h'], cinfo['quote']['USD']['percent_change_7d'],
                      cinfo['quote']['USD']['percent_change_30d'], cinfo['quote']['USD']['percent_change_60d'], cinfo['quote']['USD']['percent_change_90d'],
                      cinfo['quote']['USD']['volume_24h'], cinfo['quote']['USD']['volume_change_24h'])
            self.db.execute(insert_string, values)

    def get_info(self, ticker=None, id=None, slug=None):
        if ticker is None and id is None and slug is None:
            log.error(f'You must specify a symbol, id, or slug to seach for')
            return None
        elif ticker is not None:
            query_string = f"""SELECT * FROM {self.coininfo_table} WHERE symbol = '{ticker.upper()}'"""
        elif id is not None:
            query_string = f"""SELECT * FROM {self.coininfo_table} WHERE id = '{id}'"""
        elif slug is not None:
            query_string = f"""SELECT * FROM {self.coininfo_table} WHERE id = '{slug}'"""
        if ticker is not None:
            self.update_coin_info(ticker=ticker)
        else:
            self.update_coin_info()
        coin_info = pd.read_sql_table(self.coininfo_table, self.db)
        if coin_info is None:
            log.warning(f'No info found for coin [{ticker.upper()}] in [{self.coininfo_table}]')
            return None
        else:
            coin_info.set_index('symbol', inplace=True)
            row = coin_info.loc[ticker.upper()].to_dict()
            return row

    def get_latest(self, ticker=None, id=None, slug=None, rank=None):
        if ticker is None and id is None and slug is None and rank is None:
            log.error(f'You must specify a [symbol], [id], [slug], or [rank] to seach for')
            return None
        elif ticker is not None:
            self.update_latest_info()
            log.info(f'Retreiving coin info for symbol: [{ticker.upper()}]')
            coin_info = pd.read_sql_table(self.top200_table, self.db)
            if coin_info is None:
                log.warning(f'No info found for coin symbol [{ticker.upper()}] in [{self.top200_table}]')
                return None
            else:
                coin_info.set_index('symbol', inplace=True)
                row = coin_info.loc[ticker.upper()].to_dict()
                return row
        elif id is not None:
            self.update_latest_info()
            log.info(f'Retreiving coin info for id: [{id}]')
            coin_info = pd.read_sql_table(self.top200_table, self.db)
            if coin_info is None:
                log.warning(f'No info found for coin id [{id}] in [{self.top200_table}]')
                return None
            else:
                coin_info.set_index('id', inplace=True)
                row = coin_info.loc[id].to_dict()
                return row
        elif slug is not None:
            self.update_latest_info()
            log.info(f'Retreiving coin info for slug: [{slug}]')
            coin_info = pd.read_sql_table(self.top200_table, self.db)
            if coin_info is None:
                log.warning(f'No info found for coin slug [{slug}] in [{self.top200_table}]')
                return None
            else:
                coin_info.set_index('slug', inplace=True)
                row = coin_info.loc[slug].to_dict()
                return row
        elif rank is not None:
            self.update_latest_info()
            log.info(f'Retreiving coin info for rank: [#{rank}]')
            coin_info = pd.read_sql_table(self.top200_table, self.db)
            if coin_info is None:
                log.warning(f'No info found for coin rank [#{rank}] in [{self.top200_table}]')
                return None
            else:
                coin_info.set_index('cmc_rank', inplace=True)
                row = coin_info.loc[rank].to_dict()
                return row


if __name__ == "__main__":
    coin = CoinInfo()
    print(coin.get_info(ticker='doge'))
