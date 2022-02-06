#!/usr/bin/env python3

import json

import keys
from loguru import logger as log
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects


class CoinMarketBase:
    def __init__(self):
        self.headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': keys.cmb_apikey}
        self.session = Session()
        self.session.headers.update(self.headers)
        self.urlbase = 'https://pro-api.coinmarketcap.com/v1/'

    def get_info(self, ticker):
        url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/info'
        parameters = {'symbol': ticker.upper()}
        try:
            response = self.session.get(url, params=parameters)
            data = json.loads(response.text)
            return data
        except (ConnectionError, Timeout, TooManyRedirects) as e:
            log.error(f'Error requesting data from Coinmarketbase [{e}]')

    def get_latest_listings(self, limit=200):
        url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
        parameters = {'limit': limit,
                      'aux': 'num_market_pairs,cmc_rank,max_supply,circulating_supply,total_supply'}
        try:
            response = self.session.get(url, params=parameters)
            data = json.loads(response.text)
            return data
        except (ConnectionError, Timeout, TooManyRedirects) as e:
            log.error(f'Error requesting data from Coinmarketbase [{e}]')


if __name__ == "__main__":
    from pprint import pprint
    coin = CoinMarketBase()
    p = coin.get_latest_listings()
    pprint(p)
