import requests
import json
import datetime
import time
import pytz

from library.bootstrap import Constants
from library.utilities.file import parse_wildcards


class AlphaVantageAPI:
    _BASE_URL = 'https://www.alphavantage.co/query'

    QUOTE_URL_TEMPLATE = '{}?function=GLOBAL_QUOTE&symbol=%s%&apikey=%k%'.format(_BASE_URL)
    HISTORICAL_DAILY_URL_TEMPLATE = '{}?function=TIME_SERIES_INTRADAY&symbol=%s%&interval=5min&apikey=%k%&outputsize=full'\
        .format(_BASE_URL)

    NAME = 'AlphaVantage'
    DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    URL_TEMPLATE = 'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=%s%&apikey=%k%'
    SYMBOL_WILDCARD = '%s%'
    API_KEY_WILDCARD = '%k%'
    PRICE_WILDCARD = '%p%'

    QUOTE_RESPONSE_PARENT = 'Global Quote'
    QUOTE_SYMBOL = '01. symbol'
    QUOTE_PRICE = '05. price'
    QUOTE_VOLUME = '06. volume'

    HISTORICAL_META_DATA_RESPONSE_PARENT = 'Meta Data'
    HISTORICAL_META_DATA_TIMEZONE = '6. Time Zone'

    HISTORICAL_DAILY_RESPONSE_PARENT = 'Time Series (5min)'
    HISTORICAL_DAILY_CLOSE = '4. close'
    HISTORICAL_DAILY_VOLUME = '5. volume'


class TickerDataSource:
    _REQUEST_LIMIT = 5
    _REQUEST_LIMIT_WINDOW_MINUTES = 1

    SYMBOL = 'symbol'
    PRICE = 'price'
    VOLUME = 'volume'

    def __init__(self, cache=True):
        # TODO Caching response allows for more chaotic requests.
        self._use_cache = cache
        self._request_history = {}
        self._request_window_count = 0
        self._last_request_datetime = None

        self.name = AlphaVantageAPI.NAME
        if Constants.log:
            Constants.log.info('Data Source: Initiated {0}'.format(self.name))

    def __str__(self):
        return 'Data Source: {0}'.format(self.name)

    def _call_api(self, url):
        if self._use_cache and url in self._request_history:
            if Constants.debug:
                Constants.log.info('Using cached request results.')
            return self._request_history[url]

        # Automatically manage request limit.
        if self._request_window_count >= self._REQUEST_LIMIT:
            time_out = self._last_request_datetime + datetime.timedelta(minutes=self._REQUEST_LIMIT_WINDOW_MINUTES)
            if time_out > datetime.datetime.now():
                remaining_seconds = int((time_out - datetime.datetime.now()).total_seconds())
                Constants.log.warning('Request limit exceeded, waiting {} seconds.'.format(remaining_seconds))
                time.sleep(remaining_seconds)
            self._request_window_count = 0

        if Constants.debug:
            Constants.log.warning('Requesting: {}'.format(url))

        results = requests.get(url)
        if results.status_code == 200:
            data = json.loads(results.content.decode('utf-8'))
            self._request_history[url] = data
            self._request_window_count += 1
            self._last_request_datetime = datetime.datetime.now()
            return data
        else:
            raise Exception('Data Source: Bad status code {0}. {1}'.format(results.status_code, json.loads(results)))

    def request_count(self):
        return len(self._request_history)

    def request_quote(self, symbol):
        # Generate request URL.
        api_key = 'DemoKey'
        wildcards = {AlphaVantageAPI.SYMBOL_WILDCARD: symbol, AlphaVantageAPI.API_KEY_WILDCARD: api_key}
        url = parse_wildcards(AlphaVantageAPI.QUOTE_URL_TEMPLATE, wildcards)

        # Call API.
        results = self._call_api(url)

        if AlphaVantageAPI.QUOTE_RESPONSE_PARENT in results:
            # Extract data and return volume, price and symbol in dict
            return {
                TickerDataSource.SYMBOL: results[AlphaVantageAPI.QUOTE_RESPONSE_PARENT][AlphaVantageAPI.QUOTE_SYMBOL],
                TickerDataSource.PRICE: float(results[AlphaVantageAPI.QUOTE_RESPONSE_PARENT][AlphaVantageAPI.QUOTE_PRICE]),
                TickerDataSource.VOLUME: int(results[AlphaVantageAPI.QUOTE_RESPONSE_PARENT][AlphaVantageAPI.QUOTE_VOLUME])
            }
        else:
            Constants.log.warning('Data Source: Bad response. {0}'.format(results))
            return None

    def request_historical_data(self, symbol):
        # Generate request URL.
        api_key = 'DemoKey'
        wildcards = {AlphaVantageAPI.SYMBOL_WILDCARD: symbol, AlphaVantageAPI.API_KEY_WILDCARD: api_key}
        url = parse_wildcards(AlphaVantageAPI.HISTORICAL_DAILY_URL_TEMPLATE, wildcards)

        # Call API.
        results = self._call_api(url)

        # Extract data and return volume, price and symbol in dict
        if AlphaVantageAPI.HISTORICAL_DAILY_RESPONSE_PARENT in results:
            formatted_results = []
            for result in results[AlphaVantageAPI.HISTORICAL_DAILY_RESPONSE_PARENT]:
                # Convert timestamp to local timezone.
                timestamp_timezone = results[
                    AlphaVantageAPI.HISTORICAL_META_DATA_RESPONSE_PARENT][AlphaVantageAPI.HISTORICAL_META_DATA_TIMEZONE]
                timestamp = datetime.datetime.strptime(result, AlphaVantageAPI.DATETIME_FORMAT).replace(
                    tzinfo=pytz.timezone(timestamp_timezone))

                # Extract data.
                result = results[AlphaVantageAPI.HISTORICAL_DAILY_RESPONSE_PARENT][result]
                formatted_results.append({
                    # TODO Convert timezone (specified in meta data).
                    'timestamp': timestamp.astimezone(pytz.timezone(Constants.TIME_ZONE)),
                    TickerDataSource.SYMBOL: symbol.upper(),
                    TickerDataSource.PRICE: float(result[AlphaVantageAPI.HISTORICAL_DAILY_CLOSE]),
                    TickerDataSource.VOLUME: int(result[AlphaVantageAPI.HISTORICAL_DAILY_VOLUME])
                })
        else:
            Constants.log.warning('Data Source: Bad response. {0}'.format(results))
            return None
        return formatted_results
