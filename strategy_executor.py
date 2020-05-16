import datetime
import optparse
import os
import sys
import time

import library.strategy_functions as strategy_functions
from library.bootstrap import Constants
from library.data_loader import DataLoader
from library.interfaces.exchange import AlpacaInterface
from library.interfaces.market_data import TickerDataSource
from library.interfaces.sql_database import Database, generate_unique_id
from library.utilities.file import parse_configs_file
from library.utilities.job import Job
from library.utilities.log import get_log_file_path, setup_log, log_configs
from library.utilities.strategy import parse_strategy_from_xml

NS = {
    'XML_RISK_PROFILE_LABEL': 'execution/risk_profile',
    'XML_CHECK_LABEL': 'execution/risk_profile/check',
    'XML_CHECK_ATTRIBUTES': {
        'NAME': 'name',
        'THRESHOLD': 'threshold'
    },
    'XML_PARAMETER_LABEL': 'execution/function/parameter',
    'XML_PARAMETER_ATTRIBUTES': {
        'KEY': 'key',
        'VALUE': 'value'
    },
    'XML_FUNCTION_LABEL': 'execution/function',
    'XML_FUNCTION_ATTRIBUTES': {
        'FUNC': 'func'
    }
}


class TradeExecutor:

    def __init__(self, db, portfolio_id, exchange):
        self._db = db

        # Load latest portfolio data.
        portfolio_row = db.get_one_row('portfolios', 'id="{0}"'.format(portfolio_id))
        asset_rows = db.query_table('assets', 'portfolio_id="{0}"'.format(portfolio_row[0]))

        self.portfolio = {"id": portfolio_id,
                          "assets": {r[2]: {'symbol': r[2], 'units': int(r[3]), 'current_exposure': float(r[4])} for r in asset_rows},
                          "cash": float(portfolio_row[2])}
        # This is the passed exchange object, currently has nothing to do with exchange_name.
        self.exchange = exchange
        self._exchange_name = str(portfolio_row[1])

    def _get_order_data(self, order_id):
        return [o for o in self.exchange.get_orders() if o['id'] == order_id][0]

    def calculate_exposure(self, symbol):
        # Assume exposure == maximum possible loss from current position.
        # Get this from exchange?
        data = self.exchange.get_position(symbol)
        if data:
            units = int(data['qty'])
            current_value = float(data['current_price'])
            return units * current_value
        return 0.0

    def sync_portfolio_with_exchange(self):
        # Sync weighted cash value for strategy portfolio.
        weighting = float(self._db.get_one_row('portfolios', 'id="{0}"'.format(self.portfolio['id']))[3])
        cash = self.exchange.get_cash()
        if cash:
            self.portfolio['cash'] = cash * weighting
        else:
            raise Exception('Could not sync portfolio with exchange.')

        # Sync with exchange too.
        for symbol in self.portfolio['assets']:
            asset = self.portfolio['assets'][symbol]
            position = self.exchange.get_position(symbol=asset['symbol'])
            if position and 'qty' in position:
                asset['units'] = int(position['qty'])
            else:
                asset['units'] = 0
                if Constants.log:
                    Constants.log.warning('No position found on exchange for {0}'.format(asset['symbol']))
            asset['current_exposure'] = self.calculate_exposure(asset['symbol'])

    def meets_risk_profile(self, strategy, proposed_trade, risk_profile):
        # Only accounts for this signal, is not aware of other signals.
        signal, symbol, no_of_units, target_price = proposed_trade
        current_exposure = sum([float(self.portfolio['assets'][a]['current_exposure']) for a in self.portfolio['assets']])

        if 'max_exposure' in risk_profile:
            if signal == 'buy':
                potential_exposure = current_exposure + (no_of_units * target_price)
                if potential_exposure > float(risk_profile['max_exposure']):
                    return False
        if 'min_liquidity' in risk_profile:
            return True
        return True

    def propose_trades(self, strategy, signals, risk_profile):
        # works fine for now as each run only processes one signal. And atm my strats will only buy/sell one asset.
        self.sync_portfolio_with_exchange()
        trades = []
        for signal in signals:
            units = 1
            good_trade = True
            if signal.signal != 'hold':
                # Create trade tuple.
                trade = (signal.signal, signal.symbol, units, signal.target_value)

                # Check symbol is in portfolio.
                if good_trade and signal.symbol not in self.portfolio['assets']:
                    # Asset not found in portfolio.
                    good_trade = False

                # Check portfolio has enough units to sell.
                if good_trade and signal.signal == 'sell':
                    units_held = self.portfolio['assets'][signal.symbol]['units']
                    if units_held - units < 0:
                        # Don't sell assets you don't have.
                        good_trade = False

                # Check portfolio has sufficient capital.
                if good_trade and signal.signal == 'buy':
                    # Calculate required capital.
                    required_capital = units * signal.target_value

                    # Ensure portfolio's capital is up-to-date.
                    if required_capital > float(self.portfolio['cash']):
                        # Raise Required capital has exceeded limits.
                        good_trade = False

                # Check trade satisfies risk requirements.
                if good_trade and (not self.meets_risk_profile(strategy, trade, risk_profile)):
                    good_trade = False

                if good_trade:
                    trades.append(trade)
        return trades

    def execute_trades(self, requested_trades):
        # Return actual achieved trades, Not all trades will be fulfilled.
        executed_trade_ids = []
        for trade in requested_trades:
            signal, symbol, units, target_value = trade
            if signal == 'sell':
                executed_trade_id = self.exchange.ask(symbol, units)
            if signal == 'buy':
                executed_trade_id = self.exchange.bid(symbol, units)
            executed_trade_ids.append(executed_trade_id)
        return executed_trade_ids

    def process_executed_trades(self, executed_trade_ids, log):
        processed_trades = []
        for order_id in executed_trade_ids:
            data = self._get_order_data(order_id)
            status = data['status']

            # Wait for order to fill.
            while status == 'new' or status == 'partially_filled':
                time.sleep(1)
                data = self._get_order_data(order_id)
                status = data['status']

            # Catches bad trades.
            trade = None

            # Create order tuple with trade results.
            if status == 'filled':
                trade = (data['symbol'], int(data['filled_qty']), float(data['filled_avg_price']))

            if trade:
                # Update portfolio capital.
                change_in_capital = (int(data['filled_qty']) * float(data['filled_avg_price'])) * 1 if data['side'] == 'sell' else -1
                self.portfolio['cash'] += change_in_capital

                # Update portfolio assets.
                change_in_units = int(trade[1]) * 1 if data['side'] == 'buy' else -1
                self.portfolio['assets'][data['symbol']]['units'] += change_in_units

                # Add to processed trades list.
                processed_trades.append(trade)
            else:
                log.warning('Order {0} [{1} * {2}] failed. status: {3}'.format(order_id, data['qty'], data['symbol'], status))
        return processed_trades

    def update_portfolio_db(self, ds, append_to_historical_values=True):
        # Ensure capital is up-to-date with exchange.
        self.sync_portfolio_with_exchange()

        # Save to database.

        # Add new row for portfolio with updated capital.
        self._db.update_value('portfolios', 'cash', self.portfolio['cash'], 'id="{}"'.format(self.portfolio['id']))

        # Update assets.
        for symbol in self.portfolio['assets']:
            units = int(self.portfolio['assets'][symbol]['units'])
            self._db.update_value('assets', 'units', units, 'symbol="{}"'.format(symbol))

            # Calculate and update exposure.
            self._db.update_value('assets', 'current_exposure', self.calculate_exposure(symbol), 'symbol="{}"'.format(symbol))

        if append_to_historical_values:
            # Valuate portfolio and record in database.
            tickers = ds.request_tickers([a for a in self.portfolio['assets']])
            total_current_value_of_assets = sum([self.portfolio['assets'][asset]['units'] * float(tickers[asset])
                                                 for asset in self.portfolio['assets']])
            portfolio_value = self.portfolio['cash'] + total_current_value_of_assets
            now = datetime.datetime.now()
            self._db.insert_row('historical_portfolio_valuations', [
                generate_unique_id(now),
                self.portfolio['id'],
                now.strftime('%Y%m%d%H%M%S'),
                portfolio_value
            ])


class SignalGenerator:

    def __init__(self, ds):
        self._ds = ds

        self.strategy_name = None
        self.risk_profile = None

    def generate_signals(self, strategy_dict):
        # Build strategy context.
        context = StrategyContext(strategy_dict['name'], strategy_dict['run_datetime'], self._ds)
        parameters = strategy_dict['parameters']
        context.data = strategy_dict['data']

        # Check method exists.
        if strategy_dict['function'] not in dir(strategy_functions):
            raise Exception('Strategy function "{0}" not found.'.format(strategy_dict['function']))
        try:
            # Evaluate function.
            signals = eval('strategy_functions.{0}(context,parameters)'.format(strategy_dict['function']))
        except Exception as error:
            signals = error
            Constants.log.error('Error evaluating strategy "{0}": {1}'.format(strategy_dict['name'], error))

        return signals

    @staticmethod
    def clean_signals(dirty_signals):
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
                if unanimous_signal == 'buy':
                    # Buy for cheapest ask.
                    final_signal_index = target_values.index(min(target_values))
                elif unanimous_signal == 'sell':
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


class StrategyContext:

    def __init__(self, strategy_name, run_datetime, ds=None):
        self.now = run_datetime
        self.db = Database(Constants.configs['db_root_path'], 'algo_trading_platform', Constants.configs['environment'])
        self.data = None
        self.ds = ds if ds else None
        self.strategy_name = strategy_name
        self.signals = []

    def _generate_variable_id(self, variable_name):
        # Variables have to be unique with in a a strategy.
        # Different strategies can use the same variable names with out clashes.
        # TODO Use a consistent hash, python hash function not suitable.
        return str(self.strategy_name + variable_name)

    def add_signal(self, symbol, order_type='hold', target_value=None):
        signal = Signal(len(self.signals))
        if order_type.lower() == 'hold':
            signal.hold(symbol)
        elif order_type.lower() == 'buy' and target_value:
            signal.buy(symbol, target_value)
        elif order_type.lower() == 'sell' and target_value:
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


class Signal:

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
        self.signal = 'sell'
        self.target_value = price

    def buy(self, symbol, price):
        self.symbol = symbol
        self.signal = 'buy'
        self.target_value = price

    def hold(self, symbol):
        self.symbol = symbol
        self.signal = 'hold'
        self.target_value = None


def generate_risk_profile(db, strategy, risk_appetite=1.0):
    # Returns risk profile, dict of factor: values.
    risk_profile = {}
    # Get risk profile for strategy.
    condition = 'name="{0}"'.format(strategy)
    risk_profile_id = db.get_one_row('strategies', condition)[2]
    condition = 'id="{0}"'.format(risk_profile_id)
    headers = [h[1] for h in db.execute_sql('PRAGMA table_info(risk_profiles);')]
    risk_profile_row = [float(v) for v in db.get_one_row('risk_profiles', condition)]

    # Package risk profile into a dictionary.
    risk_profile_dict = dict(zip(headers[1:], risk_profile_row[1:]))
    for name in risk_profile_dict:
        if 'max' in name:
            risk_profile_dict[name] = risk_profile_dict[name] * risk_appetite
        if 'min' in name:
            if risk_appetite > 1:
                multiplier = 1 - (risk_appetite - 1)
            else:
                multiplier = (1 - risk_appetite) + 1
            risk_profile_dict[name] = risk_profile_dict[name] * multiplier

    risk_profile[strategy] = risk_profile_dict
    return risk_profile


def parse_cmdline_args(app_name):
    parser = optparse.OptionParser()
    parser.add_option('-e', '--environment', dest="environment")
    parser.add_option('-r', '--root_path', dest="root_path")
    parser.add_option('-j', '--job_name', dest="job_name", default=None)
    parser.add_option('--debug', dest="debug", action="store_true", default=False)
    parser.add_option('--dry_run', action="store_true", default=False)

    # Initiate script specific args.
    # Specify "simulate" or "execute" modes.
    parser.add_option('-m', '--mode', dest="mode")
    parser.add_option('-x', '--xml_path', dest="xml_path")

    options, args = parser.parse_args()
    return parse_configs_file({
        "app_name": app_name,
        "environment": options.environment.lower(),
        "root_path": options.root_path,
        "job_name": options.job_name,
        "script_name": str(os.path.basename(sys.argv[0])).split('.')[0],
        "dry_run": options.dry_run,
        "debug": options.debug,

        # Parse script specific args.
        "mode": options.mode,
        "xml_path": options.xml_path
    })


def main():
    # Setup configs.
    Constants.configs = parse_cmdline_args('algo_trading_platform')

    # Setup logging.
    log_path = get_log_file_path(Constants.configs['logs_root_path'], Constants.configs['job_name'])
    Constants.log = setup_log(log_path, True if Constants.configs['environment'] == 'dev' else False)
    log_configs(Constants.configs)

    # Setup database.
    db = Database(Constants.configs['db_root_path'], 'algo_trading_platform', Constants.configs['environment'])
    db.log()

    # Initiate Job
    job = Job()
    job.log()

    # Parse strategy xml.
    strategy_dict = parse_strategy_from_xml(Constants.configs['xml_path'])
    db.update_value('strategies', 'updated_by', job.id, 'name="{}"'.format(strategy_dict['name'].lower()))

    # Load required data sets.
    data_loader = DataLoader()
    data_loader.load_from_xml(Constants.configs['xml_path'], strategy_dict['run_datetime'])
    strategy_dict['data'] = data_loader.data

    # Generate signals.
    job.update_phase('Generating signals')
    signal_generator = SignalGenerator(TickerDataSource())
    signals = signal_generator.generate_signals(strategy_dict)

    # Clean signals.
    job.update_phase('Processing signals')
    cleaned_signals = signal_generator.clean_signals(signals)
    if not cleaned_signals:
        # Script cannot go any further from this point, but should not error.
        job.finished(condition='no valid signals')
        return 2

    Constants.log.info('Generated {0} valid signal(s).'.format(len(cleaned_signals)))
    for signal in cleaned_signals:
        Constants.log.info(str(signal))

    # Initiate exchange.
    if Constants.configs['mode'] == 'simulate':
        exchange = AlpacaInterface(Constants.configs['API_ID'], Constants.configs['API_SECRET_KEY'], simulator=True)
    elif Constants.configs['mode'] == 'execute':
        exchange = AlpacaInterface(Constants.configs['API_ID'], Constants.configs['API_SECRET_KEY'])
    else:
        # Script cannot go any further from this point.
        raise Exception('Mode "{0}" is not valid.'.format(Constants.configs['mode']))
    if not exchange.is_exchange_open():
        # Script cannot go any further from this point, but should not error.
        job.finished(condition='exchange is closed')
        return 2

    # Initiate trade executor.
    job.update_phase('Proposing_trades')
    portfolio_id = db.get_one_row('strategies', 'name="{0}"'.format(strategy_dict['name'].lower()))[2]
    trade_executor = TradeExecutor(db, portfolio_id, exchange)

    # Prepare trades.
    proposed_trades = trade_executor.propose_trades(strategy_dict['name'].lower(), cleaned_signals, strategy_dict['risk_profile'])

    # Execute trades.
    job.update_phase('Executing_trades')
    executed_order_ids = trade_executor.execute_trades(proposed_trades)

    # Process trades.
    job.update_phase('Processing_trades')
    processed_trades = trade_executor.process_executed_trades(executed_order_ids, Constants.log)

    Constants.log.info('Updated portfolio in database.')
    trade_executor.update_portfolio_db(TickerDataSource())

    # Log summary.
    Constants.log.info('Executed {0}/{1} trades successfully.'.format(len(processed_trades), len(executed_order_ids)))

    job.finished()


if __name__ == "__main__":
    sys.exit(main())

