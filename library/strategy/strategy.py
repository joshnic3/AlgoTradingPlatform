import datetime
import pytz

import library.strategy.functions as strategy_functions
from library.bootstrap import Constants
from library.data_loader import MarketDataLoader
from library.strategy.context import Context
from library.strategy.portfolio import Portfolio
from library.strategy.risk_profile import RiskProfile
from library.strategy.signal import Signal
from library.utilities.xml import get_xml_root, get_xml_element_attribute, get_xml_element_attributes


def parse_strategy_from_xml(xml_path, return_object=False, db=None):
    # Get XML root.
    root = get_xml_root(xml_path)

    # Extract strategy name.
    strategy_name = get_xml_element_attribute(root, 'name', required=True).lower()

    # Extract execution options.
    execution_element = root.findall(Constants.xml.execution.root)[0]
    execution_options_str = get_xml_element_attribute(execution_element, 'options')
    execution_options = [o.lower() for o in execution_options_str.split(',')] if execution_options_str else None

    # Extract run time.
    run_datetime_str = get_xml_element_attribute(root, 'run_datetime')
    run_datetime = datetime.datetime.strftime(run_datetime_str, Constants.date_time_format) if run_datetime_str else datetime.datetime.now()

    # Extract function name.
    function = [t for t in root.findall(Constants.xml.execution.function)][0]
    function = get_xml_element_attribute(function, 'name', required=True)

    # Parse parameters. {key: value}
    parameter_elements = [t for t in root.findall(Constants.xml.execution.parameter)]
    parameters = {i['key']: i['value'] for i in [get_xml_element_attributes(e) for e in parameter_elements]}

    # Parse risk profile. {check name: check_threshold}
    check_elements = [t for t in root.findall(Constants.xml.execution.check)]
    check_attributes = [get_xml_element_attributes(e) for e in check_elements]
    risk_profile_dict = {RiskProfile.CHECKS: {a[RiskProfile.CHECK]: a[RiskProfile.THRESHOLD] for a in check_attributes}}

    # Extract data requirements.
    # Extract ticker requirements.
    ticker_attributes = ['symbol']
    tickers = [get_xml_element_attributes(t, require=ticker_attributes) for t in root.findall(
        Constants.xml.data_requirements.ticker)]
    for ticker in tickers:
        ticker['type'] = MarketDataLoader.TICKER
        if 'before' in ticker:
            ticker['before'] = datetime.datetime.strptime(ticker['before'], Constants.DATETIME_FORMAT)
        if 'after' in ticker:
            ticker['after'] = datetime.datetime.strptime(ticker['after'], Constants.DATETIME_FORMAT)
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
    if not root.findall(Constants.xml.setup.root):
        return None

    # Extract jobs.
    job_attributes = ['name', 'script', 'schedule']
    jobs = [get_xml_element_attributes(j, require=job_attributes) for j in root.findall(Constants.xml.setup.job)]

    # Extract cash and allocation percentage.
    cash_elements = [t for t in root.findall(Constants.xml.setup.cash)]
    cash = sum([float(get_xml_element_attribute(c, 'value', required=True)) for c in cash_elements])
    allocation = sum([float(get_xml_element_attribute(c, 'allocation', required=True)) for c in cash_elements])

    # Extract assets.
    asset_attributes = ['symbol', 'units']
    assets = [get_xml_element_attributes(a, require=asset_attributes) for a in root.findall(Constants.xml.setup.asset)]

    # Return setup as dict.
    return {
        'jobs': jobs,
        'cash': cash,
        'allocation': allocation,
        'assets': assets
    }


class Strategy:
    _TABLE = 'strategies'

    def __init__(self, db, name, data_requirements, function, parameters, risk_profile, execution_options=None):
        self._db = db
        self._data_requirements = data_requirements
        self._execution_function = function
        self._execution_parameters = parameters
        self._live_data_source = None
        self._data_warning_cache = 0

        self.name = name.lower()
        # This will be overridden for regression testing.
        self.run_datetime = Constants.run_time.replace(tzinfo=None)

        # Load portfolio.
        portfolio_id = self._db.get_one_row(self._TABLE, 'name="{0}"'.format(self.name))[2]
        self.portfolio = Portfolio(portfolio_id, self._db)

        self.data_loader = MarketDataLoader()
        self.risk_profile = risk_profile
        self.execution_options = execution_options

    def load_required_data(self, historical_data=False):
        # Load required data sets.
        for required_data_set in self._data_requirements:
            if required_data_set['type'] == MarketDataLoader.TICKER:
                this_morning = self.run_datetime.replace(hour=0, minute=0, second=0)
                self.data_loader.load_tickers(
                    required_data_set['symbol'],
                    required_data_set['before'] if 'before' in required_data_set else self.run_datetime,
                    required_data_set['after'] if 'after' in required_data_set else this_morning,
                    required=4,
                    historical_data=historical_data
                )
        if self.data_loader.data:
            Constants.log.info('Loaded {0} data set(s).'.format(len(self.data_loader.data)))
        else:
            Constants.log.info('No data sets loaded.')
        if len(self.data_loader.warnings) > self._data_warning_cache:
            self._data_warning_cache = len(self.data_loader.warnings)
            Constants.log.warning('Data loader reported {0} warning(s)'.format(len(self.data_loader.warnings)))

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
        if not self.data_loader.data:
            self.load_required_data()

        # Check method exists.
        if self._execution_function not in dir(strategy_functions):
            raise Exception('Strategy function "{0}" not found.'.format(self._execution_function))

        # Attempt to execute function.
        try:
            # Build strategy context.
            context = Context(self._db, self.name, self.run_datetime, self.data_loader.data,
                                      self._live_data_source)
            parameters = self._execution_parameters
            signals = eval('strategy_functions.{0}(context,parameters)'.format(self._execution_function))
            return self._clean_signals(signals)
        except Exception as error:
            signals = error
            Constants.log.error('Error evaluating strategy "{0}": {1}'.format(self.name, error))
            return None







