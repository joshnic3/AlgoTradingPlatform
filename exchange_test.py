import json

import requests

API_KEY_ID = 'PKNPFMXUXYSXPKJ9LZM3'
SECRET_KEY = 'RJuL9bIcKg9pOKyMFZ6Gf3Z3D1bs8N9jULTETU6e'


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

    def get_orders(self):
        data = {"status": "all"}
        results = requests.get(self.api['ORDERS'], params=data, headers=self.headers, )
        return json.loads(results.content)


exchange = Exchange(API_KEY_ID, SECRET_KEY, True)
orders = exchange.get_orders()
print(orders)





schema = {}

schema['algo_trading_platform']