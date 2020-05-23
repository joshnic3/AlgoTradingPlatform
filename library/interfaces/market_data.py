import requests
import json

from library.bootstrap import Constants
from library.utilities.file import parse_wildcards


class AlphaVantageAPI:
    NAME = 'AlphaVantage'
    URL_TEMPLATE = 'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=%s%&apikey=%k%'
    SYMBOL_WILDCARD = '%s%'
    API_KEY_WILDCARD = '%k%'
    PRICE_WILDCARD = '%p%'
    RESPONSE_PARENT = 'Global Quote'
    SYMBOL = '01. symbol'
    PRICE = '05. price'
    VOLUME = '06. volume'


class TickerDataSource:
    SYMBOL = 'symbol'
    PRICE = 'price'
    VOLUME = 'volume'

    def __init__(self):
        self.name = AlphaVantageAPI.NAME
        self._request_history = {}
        if Constants.log:
            Constants.log.info('Data Source: Initiated {0}'.format(self.name))

    def __str__(self):
        return 'Data Source: {0}'.format(self.name)

    @staticmethod
    def _prepare_api_call_url(symbol):
        api_key = 'DemoKey'
        wildcards = {AlphaVantageAPI.SYMBOL_WILDCARD: symbol, AlphaVantageAPI.API_KEY_WILDCARD: api_key}
        return parse_wildcards(AlphaVantageAPI.URL_TEMPLATE, wildcards)

    def _call_api(self, url):
        if Constants.configs['debug']:
            Constants.log.warning()

        results = requests.get(url)
        if results.status_code == 200:
            data = json.loads(results.content.decode('utf-8'))
            self._request_history[url] = data
            return data
        else:
            raise Exception('Data Source: Bad status code {0}. {1}'.format(results.status_code, json.loads(results)))

    def request_count(self):
        return len(self._request_history)

    def request_quote(self, symbol):
        url = self._prepare_api_call_url(symbol.upper())
        results = self._call_api(url)

        if AlphaVantageAPI.RESPONSE_PARENT in results:
            # Extract data and return volume, price and symbol in dict
            return {
                TickerDataSource.SYMBOL: results[AlphaVantageAPI.RESPONSE_PARENT][AlphaVantageAPI.SYMBOL],
                TickerDataSource.PRICE: float(results[AlphaVantageAPI.RESPONSE_PARENT][AlphaVantageAPI.PRICE]),
                TickerDataSource.VOLUME: int(results[AlphaVantageAPI.RESPONSE_PARENT][AlphaVantageAPI.VOLUME])
            }
        else:
            Constants.log.warning('Data Source: Bad response. {0}'.format(results))
            return None

    def request_quotes(self, symbols):
        data = [self.request_quote(s) for s in symbols]
        return [d for d in data if d]
