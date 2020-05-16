import datetime
import xml.etree.ElementTree as et

from library.bootstrap import Constants
from library.interfaces.sql_database import Database, query_result_to_dict
from library.utilities.file import get_xml_element_attribute


class DataLoader:

    def __init__(self):
        self._db = Database(Constants.configs['db_root_path'], 'market_data', Constants.configs['environment'])
        self.type = None
        self.data = {}
        self.warnings = {}

    @staticmethod
    def _staleness(time_series, scope=1):
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
            tick_dict = query_result_to_dict([tick_row], Constants.configs['tables']['market_data']['ticks'])[0]
            tick_datetime = datetime.datetime.strptime(tick_dict['date_time'], Constants.date_time_format)
            tick_value = float(tick_dict['value'])
            ticks_time_series.append((tick_datetime, tick_value))

            # Carry out any data checks.
            if stale_scope and self._staleness(ticks_time_series, scope=int(stale_scope)):
                warnings.append('stale_ticker_{0}'.format(symbol.lower()))

        # Return data.
        return ticks_time_series, warnings

    def load_tickers(self, symbol, before, after):
        self.type = 'ticker'
        before = datetime.datetime.strftime(before, Constants.date_time_format)
        after = datetime.datetime.strftime(after, Constants.date_time_format)
        self.data[self.type] = {}
        self.warnings[self.type] = {}
        self.data[self.type][symbol], self.warnings[self.type][symbol] = self._load_ticks(symbol, before, after)

    def load_from_xml(self, xml_file, now=None):
        # Allow custom reference time for back testing.
        now = now if isinstance(now, datetime.datetime) else datetime.datetime.now()
        strategy = et.parse(xml_file).getroot()

        tickers = strategy.findall(Constants.xml.ticker)
        if tickers:
            self.type = 'ticker'
            self.data[self.type] = {}
            self.warnings[self.type] = {}
            for ticker in tickers:
                # Extract ticker symbol.
                symbol = get_xml_element_attribute(ticker, 'symbol', required=True)

                # Extract after datetime.
                after = get_xml_element_attribute(ticker, 'after', required=False)
                after = after if after else '00000000000000'

                # Extract before datetime.
                before = get_xml_element_attribute(ticker, 'before', required=False)
                before = before if before else datetime.datetime.strftime(now, Constants.date_time_format)

                # Extract stale scope
                stale_scope = get_xml_element_attribute(ticker, 'stale_scope', required=False)

                # Load data checks.
                data, warning = self._load_ticks(symbol, before, after, stale_scope=stale_scope)
                self.data[self.type][symbol] = data
                self.warnings[self.type][symbol] = warning



