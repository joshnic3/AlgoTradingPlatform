import datetime

import library.strategy_functions as strategy_functions
from library.bootstrap import Constants
from library.data_loader import MarketDataLoader
from library.portfolio import Portfolio
from library.risk_profile import RiskProfile
from library.utilities.xml import get_xml_root, get_xml_element_attribute, get_xml_element_attributes
from library.interfaces.sql_database import Database, query_result_to_dict


class WayPoint:
    GENERAL = 'general'
    SIGNAL = 'signal'
    TRADE = 'trade'
    VALUATION = 'valuation'

    def __init__(self, way_point_id=None, strategy=None, data=None, way_point_type=None):
        self._db = Database()

        if way_point_id:
            # Load in an existing job from database.
            way_point_row = self._db.get_one_row('strategy_way_points', 'id="{0}"'.format(way_point_id))
            way_point_table_schema = Constants.configs['tables'][Constants.db_name]['strategy_way_points']
            way_point_dict = query_result_to_dict([way_point_row], way_point_table_schema)[0]
        else:
            if strategy is None or data is None:
                raise Exception('Way point constructor requires strategy id and data if not reading database.')
            # Create new job and add it to the database.
            way_point_dict = self._create_way_point_dict(way_point_type, strategy, data)
            self._db.insert_row_from_dict('strategy_way_points', way_point_dict)

        # Set instance variables.
        self.id = way_point_dict['id']
        self.strategy_id = way_point_dict['strategy']
        self.timestamp = way_point_dict['timestamp']
        self.data = way_point_dict['data']

    @staticmethod
    def _create_way_point_dict(way_point_type, strategy, data):
        way_point_type = way_point_type if way_point_type else WayPoint.GENERAL
        return {
            'id': str(abs(hash(strategy + datetime.datetime.now().strftime(Constants.date_time_format)))),
            'strategy': strategy,
            'type': way_point_type,
            'timestamp': datetime.datetime.now().strftime(Constants.date_time_format),
            'data': data
        }


class Signal:

    HOLD = 'hold'
    SELL = 'sell'
    BUY = 'buy'

    def __init__(self, signal_id):
        self.id = signal_id
        self.symbol = None
        self.signal = None
        self.target_value = 0
        self.order_type = 'market'
        self.datetime = datetime.datetime.now()

    def __str__(self):
        market_order_pp = '' if self.signal == 'hold' else ' @ market value'
        return '[{0} {1}{2}]'.format(self.signal, self.symbol, market_order_pp)

    def __repr__(self):
        return self.__str__()

    def sell(self, symbol, price):
        self.symbol = symbol
        self.signal = Signal.SELL
        self.target_value = price

    def buy(self, symbol, price):
        self.symbol = symbol
        self.signal = Signal.BUY
        self.target_value = price

    def hold(self, symbol):
        self.symbol = symbol
        self.signal = Signal.HOLD
        self.target_value = None


class StrategyContext:

    def __init__(self, db, strategy_name, run_datetime, data, ds=None):
        self.now = run_datetime
        self.db = db
        self.data = data
        self.ds = ds if ds else None
        self.strategy_name = strategy_name
        self.signals = []

    def _generate_variable_id(self, variable_name):
        # Variables have to be unique with in a a strategy.
        # Different strategies can use the same variable names with out clashes.
        # TODO Use a consistent hash, python hash function not suitable.
        return str(self.strategy_name + variable_name)

    def add_signal(self, symbol, order_type=Signal.HOLD, target_value=None):
        signal = Signal(len(self.signals))
        if order_type.lower() == Signal.HOLD:
            signal.hold(symbol)
        elif order_type.lower() == Signal.BUY and target_value:
            signal.buy(symbol, target_value)
        elif order_type.lower() == Signal.SELL and target_value:
            signal.sell(symbol, target_value)
        else:
            raise Exception('Signal not valid.')
        self.signals.append(signal)

    def set_variable(self, name, new_value):
        variable_id = self._generate_variable_id(name)
        if self.get_variable(name):
            # update value in db.
            self.db.update_value('strategy_variables', 'value', new_value, 'id="{0}"'.format(variable_id))
        else:
            # insert new variable.
            values = [variable_id, new_value]
            self.db.insert_row('strategy_variables', values)
        return new_value

    def get_variable(self, name, default=None):
        variable_id = self._generate_variable_id(name)
        result = self.db.get_one_row('strategy_variables', 'id="{0}"'.format(variable_id))
        if result:
            return result[1]
        else:
            if default is not None:
                return self.set_variable(name, default)
            return None


class Strategy:

    def __init__(self, db, name, data_requirements, function, parameters, risk_profile, execution_options=None):
        self._db = db
        self._data_requirements = data_requirements
        self._execution_function = function
        self._execution_parameters = parameters
        self._live_data_source = None

        self.name = name.lower()
        self.run_datetime = datetime.datetime.now()

        # Load portfolio.
        portfolio_id = self._db.get_one_row('strategies', 'name="{0}"'.format(self.name))[2]
        self.portfolio = Portfolio(portfolio_id, self._db)

        self.data_loader = MarketDataLoader()
        self.risk_profile = risk_profile
        self.execution_options = execution_options

    def _load_required_data(self):
        # Load required data sets.
        for required_data_set in self._data_requirements:
            if required_data_set['type'] == MarketDataLoader.TICKER:
                after = required_data_set['after'] if 'after' in required_data_set else datetime.datetime(1970, 1, 1, 0, 0, 0)
                before = required_data_set['before'] if 'before' in required_data_set else self.run_datetime
                self.data_loader.load_tickers(required_data_set['symbol'], before, after)
        if self.data_loader.data:
            Constants.log.info('Loaded {0} data set(s).'.format(len(self.data_loader.data)))
        else:
            Constants.log.info('No data sets loaded.')

        if self.data_loader.warnings:
            Constants.log.warning('Data loader reported {0} warnings'.format(len(self.data_loader.warnings)))

    @staticmethod
    def _clean_signals(dirty_signals):
        # If there is only one signal.
        if isinstance(dirty_signals, Signal):
            return dirty_signals
        if not isinstance(dirty_signals, list):
            return None

        # Remove errors.
        signals = [ds for ds in dirty_signals if isinstance(ds, Signal)]

        # Group symbols by symbol.
        unique_symbols = list(set([s.symbol for s in signals]))
        signals_per_unique_symbol = {us: [s for s in signals if s.symbol == us] for us in unique_symbols}

        for symbol in unique_symbols:
            symbol_signals = [s.signal for s in signals_per_unique_symbol[symbol]]
            symbol_signals_set = list(set(symbol_signals))

            # If all the signals agree unify signal per symbol, else raise error for symbol.
            unanimous_signal = None if len(symbol_signals_set) > 1 else symbol_signals_set[0]
            if unanimous_signal:
                target_values = [s.target_value for s in signals_per_unique_symbol[symbol]]
                if unanimous_signal == Signal.BUY:
                    # Buy for cheapest ask.
                    final_signal_index = target_values.index(min(target_values))
                elif unanimous_signal == Signal.SELL:
                    # Sell to highest bid.
                    final_signal_index = target_values.index(max(target_values))
                else:
                    final_signal_index = 0
                signals_per_unique_symbol[symbol] = signals_per_unique_symbol[symbol][final_signal_index]
            else:
                conflicting_signals_str = ', '.join([str(s) for s in signals_per_unique_symbol[symbol]])
                raise Exception(
                    'Could not unify conflicting signals for "{0}": {1}'.format(symbol, conflicting_signals_str))

        # Return cleaned signals.
        return [signals_per_unique_symbol[signal] for signal in unique_symbols]

    def generate_signals(self):
        # load required data.
        self._load_required_data()

        # Check method exists.
        if self._execution_function not in dir(strategy_functions):
            raise Exception('Strategy function "{0}" not found.'.format(self._execution_function))

        # Attempt to execute function.
        try:
            # Build strategy context.
            context = StrategyContext(self._db, self.name, self.run_datetime, self.data_loader.data,
                                      self._live_data_source)
            parameters = self._execution_parameters
            signals = eval('strategy_functions.{0}(context,parameters)'.format(self._execution_function))
            return self._clean_signals(signals)
        except Exception as error:
            signals = error
            Constants.log.error('Error evaluating strategy "{0}": {1}'.format(self.name, error))
            return None


def parse_strategy_from_xml(xml_path, return_object=False, db=None):
    # Get XML root.
    root = get_xml_root(xml_path)

    # Extract strategy name.
    strategy_name = get_xml_element_attribute(root, 'name', required=True).lower()

    # Extract execution options.
    execution_element = root.findall(Constants.xml.execution)[0]
    execution_options_str = get_xml_element_attribute(execution_element, 'options')
    execution_options = [o.lower() for o in execution_options_str.split(',')] if execution_options_str else None

    # Extract run time.
    run_datetime_str = get_xml_element_attribute(root, 'run_datetime')
    run_datetime = datetime.datetime.strftime(run_datetime_str, Constants.date_time_format) if run_datetime_str else datetime.datetime.now()

    # Extract function name.
    function = [t for t in root.findall(Constants.xml.function)][0]
    function = get_xml_element_attribute(function, 'name', required=True)

    # Parse parameters. {key: value}
    parameter_elements = [t for t in root.findall(Constants.xml.parameter)]
    parameters = {i['key']: i['value'] for i in [get_xml_element_attributes(e) for e in parameter_elements]}

    # Parse risk profile. {check name: check_threshold}
    check_elements = [t for t in root.findall(Constants.xml.check)]
    check_attributes = [get_xml_element_attributes(e) for e in check_elements]
    risk_profile_dict = {RiskProfile.CHECKS: {a[RiskProfile.CHECK]: a[RiskProfile.THRESHOLD] for a in check_attributes}}

    # Extract data requirements.
    # Extract ticker requirements.
    ticker_attributes = ['symbol']
    tickers = [get_xml_element_attributes(t, require=ticker_attributes) for t in root.findall(Constants.xml.ticker)]
    for ticker in tickers:
        ticker['type'] = MarketDataLoader.TICKER
    data_requirements = tickers

    if return_object:
        if db is None:
            raise Exception('A database object must be passed in if returning object.')
        risk_profile = RiskProfile(risk_profile_dict)
        return Strategy(db, strategy_name, data_requirements, function, parameters, risk_profile,
                        execution_options=execution_options)
    else:
        return {
            'name': strategy_name,
            'run_datetime': run_datetime,
            'function': function,
            'parameters': parameters,
            'risk_profile': risk_profile_dict,
            'data_requirements': data_requirements,
            'execution_options': execution_options
        }


def parse_strategy_setup_from_xml(xml_path):
    # Get XML root.
    root = get_xml_root(xml_path)

    # Check xml has setup elements.
    if not root.findall(Constants.xml.setup):
        return None

    # Extract jobs.
    job_attributes = ['name', 'script', 'schedule']
    jobs = [get_xml_element_attributes(j, require=job_attributes) for j in root.findall(Constants.xml.job)]

    # Extract cash.
    cash_elements = [t for t in root.findall(Constants.xml.cash)]
    cash = sum([float(get_xml_element_attribute(c, 'value', required=True)) for c in cash_elements])

    # Extract assets.
    asset_attributes = ['symbol']
    assets = [get_xml_element_attributes(a, require=asset_attributes) for a in root.findall(Constants.xml.asset)]

    # Return setup as dict.
    return {
        'jobs': jobs,
        'cash': cash,
        'assets': assets
    }



