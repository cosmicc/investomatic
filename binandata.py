from datetime import datetime, timedelta

import keys
from binance.client import Client
from loguru import logger as log
from pandas import DataFrame as df


class Price:
    def __init__(self, minutes=5):
        self.refreshtime = minutes
        try:
            self.client = Client(api_key=keys.api_key, api_secret=keys.api_secret)
            self.connected = True
        except Exception as error:
            log.error(f'Error connecting to binance: {error}')
            self.connected = False
        self.prices = None
        self.last_price_time = datetime.now() - timedelta(hours=1)

    def refresh_prices(self):
        if self.last_price_time is None or datetime.now() - self.last_price_time > timedelta(minutes=self.refreshtime):
            try:
                log.debug(f'Successful refresh of price data from Binance')
                self.prices = df(self.client.get_ticker())
                self.prices.set_index('symbol', inplace=True)
                self.last_price_time = datetime.now()
                self.connected = True
            except Exception as error:
                log.error(f'Error refreshing price data on binance: {error}')
                self.connected = False

    def _columns(self):
        self.refresh_prices()
        return self.prices.columns

    def system_status(self):
        try:
            status = self.client.get_system_status()
            self.connected = True
        except Exception as error:
            log.error(f'Error connecting to binance: {error}')
            self.connected = False
        return {'lastrefresh': self.last_price_time, 'connected': self.connected, 'status': status['msg']}

    def all_prices(self):
        self.refresh_prices()
        return self.prices

    def latest_data(self, sym):
        self.refresh_prices()
        return self.prices.loc[sym]

    def price(self, sym):
        self.refresh_prices()
        return self.prices.loc[sym]['lastPrice']

    def price_change(self, sym):
        self.refresh_prices()
        return self.prices.loc[sym]['priceChange']

    def percent_change(self, sym):
        self.refresh_prices()
        return self.prices.loc[sym]['priceChangePercent']

    def weighted_price(self, sym):
        self.refresh_prices()
        return self.prices.loc[sym]['weightedAvgPrice']

    def volume(self, sym):
        self.refresh_prices()
        return self.prices.loc[sym]['volume']

    def quantity(self, sym):
        self.refresh_prices()
        return self.prices.loc[sym]['lastQty']

    def exists(self, sym):
        self.refresh_prices()
        try:
            nana = self.prices.loc[sym]
            return True
        except:
            return False

    def price_feeder(self, ticker, limit=1000, historic=False, start="", stop=""):
        log.info(f'Retreiving price data for [{ticker}] from binance')
        try:
            if not historic:
                candles = self.client.get_klines(symbol=ticker, interval=Client.KLINE_INTERVAL_15MINUTE, limit=100)
            else:
                candles = self.client.get_historical_klines(symbol=ticker, interval=Client.KLINE_INTERVAL_15MINUTE, start_str=start, end_str=stop)
                if len(candles) == 0:
                    return None
            candles_data_frame = df(candles)
            candles_data_frame_date = candles_data_frame[0]
            candles_data_frame_close = candles_data_frame[6]
            final_date = []
            for time in candles_data_frame_date.unique():
                readable = datetime.fromtimestamp(int(time / 1000))
                final_date.append(readable)
            final_date2 = []
            for time in candles_data_frame_close.unique():
                readable = datetime.fromtimestamp(int(time / 1000))
                final_date2.append(readable)
            candles_data_frame.pop(0)
            candles_data_frame.pop(6)
            candles_data_frame.pop(11)
            dataframe_final_date = df(final_date)
            dataframe_final_date.columns = ['open_time']
            dataframe_final_date2 = df(final_date2)
            dataframe_final_date2.columns = ['close_time']
            final_dataframe = dataframe_final_date.join(dataframe_final_date2)
            final_dataframe = final_dataframe.join(candles_data_frame)
            final_dataframe.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'asset_volume', 'trades', 'taker_buy_base', 'taker_buy_quote']
            final_dataframe.set_index('open_time', inplace=True)
            log.success(f'Retrieval of price data for [{ticker}] from binance complete')
            return final_dataframe
        except Exception as error:
            log.critical(f'Ticker feeder error: {error}')
