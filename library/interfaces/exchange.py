import datetime
import json
import random

import requests

from library.bootstrap import Constants
from library.data_loader import MarketDataLoader
from library.strategy.strategy import Portfolio, Signal


class AlpacaInterface:
    API_ID = 'API_ID'
    API_SECRET_KEY = 'API_SECRET_KEY'

    STATUS = 'status'
    NEW_ORDER = 'new'
    PARTIALLY_FILLED_ORDER = 'partially_filled'
    FILLED_ORDER = 'filled'
    ORDER_SIDE = 'side'
    FILLED_UNITS = 'filled_qty'
    FILLED_MEAN_PRICE = 'filled_avg_price'
    SYMBOL = 'symbol'
    UNITS = 'qty'
    CASH = 'cash'

    def __init__(self, key_id, secret_key, paper=False):
        if paper:
            base_url = 'https://paper-api.alpaca.markets'

        self.headers = {'APCA-API-KEY-ID': key_id, 'APCA-API-SECRET-KEY': secret_key}
        self.api = {
            'ACCOUNT': '{0}/v2/account'.format(base_url),
            'ORDERS': '{0}/v2/orders'.format(base_url),
            'POSITIONS': '{0}/v2/positions'.format(base_url),
            'CLOCK': '{0}/v2/clock'.format(base_url)
        }

    def _request_get(self, url, params=None, data=None, except_error=False):
        # TODO Handle response errors. Should log non-fatal responses and raise exceptions for fatal ones.
        results = requests.get(url, data=data, params=params, headers=self.headers)
        if results.status_code == 200:
            return json.loads(results.content.decode('utf-8'))
        elif except_error:
            return json.loads(results.content.decode('utf-8'))
        else:
            error_message = json.loads(results.text)['message']
            raise Exception('Response error ({0}): {1}'.format(results.status_code, error_message))

    def _create_order(self, symbol, units, side):
        # Assuming all orders at this point are valid.
        # Will offer limit orders in the future.
        data = {"symbol": symbol, "qty": units, "side": side, "type": "market", "time_in_force": "gtc"}
        # Ensure can sell if required.
        if side == 'sell' and self.get_position(symbol, 'qty') is None:
            raise Exception('There is no "{0}" in portfolio.'.format(symbol))
        results = requests.post(self.api['ORDERS'], json=data, headers=self.headers)
        # TODO Check if response has been excepted. Don't worry about the order yet, we check that later.
        return json.loads(results.content.decode('utf-8'))

    def is_exchange_open(self):
        data = self._request_get(self.api['CLOCK'])
        return data['is_open']
        # return True

    def get_order_data(self, order_id):
        orders = self._request_get(self.api['ORDERS'], params={"status": "all"})
        return [o for o in orders if o['id'] == order_id][0]

    def get_cash(self):
        data = self._request_get(self.api['ACCOUNT'])
        if 'cash' in data:
            return float(data['cash'])
        return None

    def get_position(self, symbol, key=None):
        data = self._request_get('{}/{}'.format(self.api['POSITIONS'], symbol), except_error=True)
        if 'code' in data:
            return 0
        if key in data:
            return data[key]
        return data

    def ask(self, symbol, units):
        results = self._create_order(symbol, units, Signal.SELL)
        if not results['id']:
            return None
        return results['id']

    def bid(self, symbol, units):
        results = self._create_order(symbol, units, Signal.BUY)
        if not results['id']:
            return None
        return results['id']


class SimulatedExchangeInterface(AlpacaInterface):

    def __init__(self, strategy, cash):
        AlpacaInterface.__init__(self, '', '', paper=True)

        self._portfolio = strategy.portfolio
        self._market_data_loader = strategy.data_loader
        self._run_datetime = strategy.run_datetime
        self._orders = []
        self._cash = cash

    def _get_simulated_price(self, symbol):
        if self._market_data_loader:
            self._market_data_loader.load_latest_ticker(symbol, now=self._run_datetime)
            return self._market_data_loader.data[MarketDataLoader.LATEST_TICKER][symbol]
        else:
            return random.uniform(0, 200)

    def _add_order(self, symbol, units, side):
        # Assume all orders are fully fulfilled.
        order_id = str(abs(hash(symbol + datetime.datetime.now().strftime(Constants.DATETIME_FORMAT))))
        self._orders.append({
            'id': order_id,
            AlpacaInterface.STATUS: AlpacaInterface.FILLED_ORDER,
            AlpacaInterface.FILLED_UNITS: str(units),
            AlpacaInterface.ORDER_SIDE: side,
            AlpacaInterface.FILLED_MEAN_PRICE: str(self._get_simulated_price(symbol)),
            AlpacaInterface.SYMBOL: symbol
        })
        return order_id

    def _request_get(self, url, params=None, data=None, except_error=False):
        if self.api['CLOCK'] in url:
            return True

        if self.api['POSITIONS'] in url:
            symbol = url.split('/')[-1]
            units = str(self._portfolio.assets[symbol][Portfolio.UNITS])
            return {AlpacaInterface.UNITS: units, AlpacaInterface.SYMBOL: symbol}

        if self.api['ACCOUNT'] in url:
            return {AlpacaInterface.CASH: str(self._cash)}

        if self.api['ORDERS'] in url:
            return self._orders

    def _create_order(self, symbol, units, side):
        return {'id': self._add_order(symbol, units, side)}
