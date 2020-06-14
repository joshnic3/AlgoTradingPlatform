import datetime
from collections import OrderedDict

import pytz

from library.bootstrap import Constants
from library.interfaces.market_data import TickerDataSource
from library.interfaces.sql_database import Database, query_result_to_dict
from library.strategy.bread_crumbs import BreadCrumbs
from library.utilities.onboarding import generate_unique_id


# Yes, I know this should probably use Pandas, but I've put too much time in at this point to back out.
class DataLoader:
    VALUE_DATA_TYPES = ['valuation']

    # Warning types.
    NO_DATA = 0
    INCOMPLETE_DATA = 1
    WARNINGS = [
        'no data',
        'incomplete data'
    ]

    def __init__(self, data_type, db_name=None):
        self.db = Database(Constants.db_path, Constants.environment, name=db_name)
        self.type = data_type
        self.data = {}
        self.warnings = {}

    def _add_data_or_warning(self, identifier, data=None, warning=None, override=False):
        if not data and not warning:
            raise Exception('_add_data_or_warning requires either data or warning to be passed.')

        destination = self.data if data else self.warnings
        item = data if data else warning

        if self.type not in destination:
            destination[self.type] = {identifier: []}
        if identifier not in destination[self.type]:
            destination[self.type][identifier] = item
        else:
            if override:
                destination[self.type][identifier] = item
            else:
                # This will only work with arrays
                destination[self.type][identifier] += item

    def _add_data(self, identifier, data, override=False):
        self._add_data_or_warning(identifier, data=data, override=override)

    def _add_warning(self, identifier, warning):
        self._add_data_or_warning(identifier, warning=warning)

    def report_warnings(self):
        if Constants.log:
            log_prefix = 'Data Loader: '
            if self.warnings:
                for data_type in self.warnings:
                    data_warnings = self.warnings[data_type]
                    Constants.log.warning('{}Data warning: type: {}, {}, '.format(log_prefix, data_type, data_warnings))
            else:
                Constants.log.info('{}No data warnings.'.format(log_prefix))


class BreadCrumbsDataLoader(DataLoader):
    BREAD_CRUMBS_TIME_SERIES = 'bread_crumbs_time_series'

    # Data Indexes.
    TIMESTAMP = 3
    TYPE = 2
    DATA = 4

    def __init__(self):
        DataLoader.__init__(self, self.BREAD_CRUMBS_TIME_SERIES)

    def load_bread_crumbs_time_series(self, strategy_name):
        bread_crumb_rows = self.db.query_table(BreadCrumbs.TABLE, 'strategy="{}"'.format(strategy_name))
        if bread_crumb_rows:
            self._add_data(strategy_name, bread_crumb_rows, override=True)
        else:
            warning = [Constants.run_time, self.WARNINGS[self.NO_DATA], strategy_name]
            self._add_warning(strategy_name, warning)


class MarketDataLoader(DataLoader):
    DB_NAME = 'market_data'
    TICKER = 'ticker'
    LATEST_TICKER = 'latest_ticker'

    def __init__(self):
        DataLoader.__init__(self, MarketDataLoader.TICKER, db_name=MarketDataLoader.DB_NAME)
        self.market_data_source = None

    def _load_ticks_from_database(self, symbol, before_datetime, after_datetime):

        before_string = datetime.datetime.strftime(before_datetime, Constants.DATETIME_FORMAT)
        after_string = datetime.datetime.strftime(after_datetime, Constants.DATETIME_FORMAT)
        # Read ticks from database.
        condition = 'symbol="{0}" AND date_time<"{1}" AND date_time>"{2}"'.format(symbol, before_string, after_string)
        tick_rows = self.db.query_table('ticks', condition)

        # Format required tick data into time series [(datetime, float)].
        ticks_time_series = []
        for tick_row in tick_rows:
            tick_dict = query_result_to_dict([tick_row], Constants.configs['tables'][MarketDataLoader.DB_NAME]['ticks'])[0]
            tick_datetime = datetime.datetime.strptime(tick_dict['date_time'], Constants.DATETIME_FORMAT).astimezone(
                pytz.timezone(Constants.TIME_ZONE))
            tick_value = float(tick_dict['price'])
            tick_volume = int(tick_dict['volume'])
            ticks_time_series.append((tick_datetime, tick_value, tick_volume))

        # Return tick data.
        ticks_time_series.reverse()
        return ticks_time_series

    def _load_historical_ticks(self, symbol, before, after):
        if self.market_data_source is None:
            self.market_data_source = TickerDataSource()

        # Request historical data from market data source.
        historical_data = self.market_data_source.request_historical_data(symbol)

        # Filter relevant data.
        filtered_historical_data = [t for t in historical_data if after <= t['timestamp'] <= before]

        # Save historical tick data to database.
        db = Database(name=MarketDataLoader.DB_NAME)
        for ticker in filtered_historical_data:
            timestamp = ticker['timestamp'].strftime(Constants.DATETIME_FORMAT)
            db.insert_row('ticks', [generate_unique_id(ticker['symbol'] + timestamp), timestamp, ticker['symbol'],
                                    ticker['price'], ticker['volume']])

        # Format and return historical tick data as time series.
        time_series = [(d['timestamp'], float(d['price']), int(d['volume'])) for d in filtered_historical_data]
        time_series.reverse()
        return time_series

    def load_tickers(self, symbol, before, after, required=1, historical_data=False):
        data_detail_string = '{} {} - {}'.format(symbol.upper(), after.strftime(Constants.PP_DATETIME_FORMAT),
                                                 before.strftime(Constants.PP_DATETIME_FORMAT))
        self.type = MarketDataLoader.TICKER
        if Constants.debug:
            Constants.log.info('Loading tick data for {}'.format(data_detail_string))

        # Attempt to load market data from database.
        data = self._load_ticks_from_database(symbol, before, after)

        # If data is missing, request historical data from market data source.
        if historical_data:
            if not data or len(data) < required:
                Constants.log.info('Requesting historical market data for {}.'.format(data_detail_string))
                historical_data = self._load_historical_ticks(symbol, before, after)

                # Merge database and historical market data.
                [data.append(h) for h in historical_data if h not in data]

        # Process data.
        if data:
            # Remove time zone and any duplicated whilst maintaining order.
            formatted_data = [(d[0].replace(tzinfo=None), d[1], d[2]) for d in list(OrderedDict.fromkeys(data))]
            self._add_data(symbol, formatted_data)

        # Add any warnings.
        if data and len(data) < required:
            Constants.log.warning('Only partially loaded tick data for {}'.format(data_detail_string))
            warning = [Constants.run_time, self.WARNINGS[self.INCOMPLETE_DATA], data_detail_string]
            self._add_warning(symbol, warning)
        if not data:
            Constants.log.warning('Failed to load tick data for {}'.format(data_detail_string))
            warning = [Constants.run_time, self.WARNINGS[self.NO_DATA], data_detail_string]
            self._add_warning(symbol, warning)

    # I still think this should be a separate class, I just don't know how yet.
    def load_latest_ticker(self, symbol, now=None):
        data_detail_string = '{} latest'.format(symbol.upper())
        self.type = MarketDataLoader.LATEST_TICKER
        if Constants.debug:
            Constants.log.info('Loading tick data for {}'.format(data_detail_string))

        now = now if now else Constants.run_time
        now_datetime_string = now.strftime(Constants.DATETIME_FORMAT)

        # Read tick from database.
        condition = 'symbol="{0}" AND date_time<{1}'.format(symbol, now_datetime_string)
        tick_rows = self.db.get_one_row('ticks', condition, columns='max(date_time), price')
        if tick_rows[1]:
            self._add_data(symbol, float(tick_rows[1]), override=True)
        else:
            warning = [Constants.run_time, self.WARNINGS[self.NO_DATA], data_detail_string]
            self._add_warning(symbol, warning)





