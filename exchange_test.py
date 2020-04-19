import json

import requests

API_KEY_ID = 'PKYA432HXIW1TE14MLQ9'
SECRET_KEY = 'a6W9OIsW28ySRW06ydohu7ZfdVrsj/cnljrwBhsP'


class Exchange:

    def __init__(self, key_id, secret_key, simulator=False):
        if simulator:
            base_url = 'https://paper-api.alpaca.markets'
        self.headers = {'APCA-API-KEY-ID': key_id, 'APCA-API-SECRET-KEY': secret_key}
        self.api = {
            'ACCOUNT': '{0}/v2/account'.format(base_url),
            'ORDERS': '{0}/v2/orders'.format(base_url),
            'POSITIONS': '{0}/v2/positions'.format(base_url)
        }

    def get_position(self, symbol, key=None):
        results = requests.get('{}/{}'.format(self.api['POSITIONS'], symbol), headers=self.headers)
        data = json.loads(results.content)
        if 'code' in data:
            return 0
        if key in data:
            return data[key]
        return data

    def ask(self, symbol, units):
        results = self._create_order(symbol, units, 'sell')
        if not results:
            return None

        units_ordered = results['symbol']
        filled = results['filled_qty']

        return results['id']

    def bid(self, symbol, units):
        results = self._create_order(symbol, units, 'buy')
        if not results:
            return None
        return results['id']

    def _create_order(self, symbol, units, side):
        # Assuming all orders at this point are valid.
        # Will offer limit orders in the future.
        data = {
            "symbol": symbol,
            "qty": units,
            "side": side,
            "type": "market",
            "time_in_force": "gtc"
        }
        change = units if side == 'buy' else 0 - units
        before = self.get_position(symbol, 'qty')
        if before is None:
            # No position for symbol exists.
            if side == 'sell':
                raise Exception('There is no "{0}" in portfolio.'.format(symbol))
            before = 0
        results = requests.post(self.api['ORDERS'], json=data, headers=self.headers)
        if int(before) + int(change) == int(self.get_position(symbol, 'qty')):
            return json.loads(results.content)
        return None


exchange = Exchange(API_KEY_ID, SECRET_KEY, True)
order_id = exchange.bid('AAPL', 4)
print(order_id)



