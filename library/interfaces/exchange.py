import json
import requests

from library.bootstrap import Constants


class AlpacaInterface:

    def __init__(self, key_id, secret_key, simulator=False):
        if simulator:
            base_url = 'https://paper-api.alpaca.markets'

        self.headers = {'APCA-API-KEY-ID': key_id, 'APCA-API-SECRET-KEY': secret_key}
        self.api = {
            'ACCOUNT': '{0}/v2/account'.format(base_url),
            'ORDERS': '{0}/v2/orders'.format(base_url),
            'POSITIONS': '{0}/v2/positions'.format(base_url),
            'CLOCK': '{0}/v2/clock'.format(base_url)
        }

    def _request_get(self, url, params=None, data=None, handle_error=False):
        # Handle response errors. Should log non-fatal responses and raise exceptions for fatal ones.
        results = requests.get(url, data=data, params=params, headers=self.headers)
        if results.status_code == 200:
            return json.loads(results.content.decode('utf-8'))
        elif handle_error:
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

    def get_orders(self):
        return self._request_get(self.api['ORDERS'], params={"status": "all"})

    def get_cash(self):
        data = self._request_get(self.api['ACCOUNT'])
        if 'cash' in data:
            return float(data['cash'])
        return None

    def get_position(self, symbol, key=None):
        data = self._request_get('{}/{}'.format(self.api['POSITIONS'], symbol), handle_error=True)
        if 'code' in data:
            return 0
        if key in data:
            return data[key]
        return data

    def ask(self, symbol, units):
        results = self._create_order(symbol, units, 'sell')
        if not results['id']:
            return None
        return results['id']

    def bid(self, symbol, units):
        results = self._create_order(symbol, units, 'buy')
        if not results['id']:
            return None
        return results['id']
