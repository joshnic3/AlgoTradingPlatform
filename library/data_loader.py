import datetime

from library.bootstrap import Constants
from library.interfaces.sql_database import Database, query_result_to_dict


class DataLoader:

    def __init__(self, db_name):
        self._db = Database(Constants.configs['db_root_path'], Constants.configs['environment'], name=db_name)
        self.type = None
        self.data = {}
        self.warnings = {}

    def report_warnings(self):
        if Constants.log:
            log_prefix = 'Data Loader: '
            if self.warnings:
                for data_type in self.warnings:
                    data_warnings = self.warnings[data_type]
                    Constants.log.warning('{}Data warning: type: {}, {}, '.format(log_prefix, data_type, data_warnings))
            else:
                Constants.log.info('{}No data warnings.'.format(log_prefix))


class WayPointDataLoader(DataLoader):

    DB_NAME = 'algo_trading_platform'
    WAY_POINT_TIME_SERIES = 'way_point_time_series'

    def __init__(self):
        DataLoader.__init__(self, WayPointDataLoader.DB_NAME)

    def load_way_point_time_series(self, strategy_name):
        self.type = WayPointDataLoader.WAY_POINT_TIME_SERIES
        self.data[self.type] = {strategy_name: {}}
        way_point_rows = self._db.query_table('strategy_way_points', 'strategy="{}"'.format(strategy_name))
        if way_point_rows:
            # Extract time series.
            way_point_time_series = [(way_point_row[-3:]) for way_point_row in way_point_rows]

            # Group time series by type.
            way_point_types = set([w[0] for w in way_point_time_series])
            for way_point_type in way_point_types:
                data = [[w[1], w[2]] for w in way_point_time_series if w[0] == way_point_type]
                self.data[self.type][strategy_name][way_point_type] = data
        else:
            self.warnings[self.type] = {strategy_name: 'not_in_database'}


class MarketDataLoader(DataLoader):

    DB_NAME = 'market_data'
    TICKER = 'ticker'
    LATEST_TICKER = 'latest_ticker'

    def __init__(self):
        DataLoader.__init__(self, MarketDataLoader.DB_NAME)

    @staticmethod
    def _staleness(time_series, scope=1):
        # TODO this is shit
        # Quantifies staleness
        #   time_series [(datetime, float)]
        #   Will return True if any value is the same as the next *scope* elements including itself.
        #   e.g. scope = 3,  [..., 91.61, |91.65, 91.65, 91.64|, 91.64, ...] => False

        values = [t[1] for t in time_series]
        scope = int(scope)
        stale_count = 0
        for i in range(0, len(values)):
            values_in_scope = [values[i + j] for j in range(scope)] if i + (scope - 1) < len(values) else values[i:]
            if len(values_in_scope) > 1 and (len(set(values_in_scope)) != len(values_in_scope)):
                stale_count += 1
        return stale_count / len(time_series)

    def _load_ticks(self, symbol, before, after, stale_scope=None):
        # Read ticks from database.
        condition = 'symbol="{0}" AND date_time<"{1}" AND date_time>"{2}"'.format(symbol, before, after)
        tick_rows = self._db.query_table('ticks', condition)

        # Read required data into time series [(datetime, float)].
        ticks_time_series = []
        warnings = []
        for tick_row in tick_rows:
            tick_dict = query_result_to_dict([tick_row], Constants.configs['tables'][MarketDataLoader.DB_NAME]['ticks'])[0]
            tick_datetime = datetime.datetime.strptime(tick_dict['date_time'], Constants.date_time_format)
            tick_value = float(tick_dict['value'])
            ticks_time_series.append((tick_datetime, tick_value))

            # Carry out any data checks.
            if stale_scope and self._staleness(ticks_time_series, scope=int(stale_scope)):
                warnings.append('stale_ticker_{0}'.format(symbol.lower()))

        # Return data.
        return ticks_time_series, warnings

    def load_tickers(self, symbol, before, after):
        self.type = MarketDataLoader.TICKER
        before = datetime.datetime.strftime(before, Constants.date_time_format)
        after = datetime.datetime.strftime(after, Constants.date_time_format)
        data, warnings = self._load_ticks(symbol, before, after)
        if data:
            if self.type in self.data:
                self.data[self.type][symbol] = data
            else:
                self.data[self.type] = {symbol: data}
        if warnings:
            if self.type in self.warnings:
                self.warnings[self.type][symbol] = warnings
            else:
                self.warnings[self.type] = {symbol: warnings}

    def load_latest_ticker(self, symbol, now=None):
        self.type = MarketDataLoader.LATEST_TICKER
        now = now if now else datetime.datetime.now()
        now_datetime_string = now.strftime(Constants.date_time_format)

        # Read tick from database.
        condition = 'symbol="{0}" AND date_time<{1}'.format(symbol, now_datetime_string)
        tick_rows = self._db.get_one_row('ticks', condition, columns='max(date_time), value')
        if tick_rows[1]:
            self.data[self.type] = {symbol: float(tick_rows[1])}
        else:
            self.warnings[self.type] = {symbol: 'no_ticks_{0}'.format(symbol.lower())}



