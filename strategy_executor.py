import datetime
import optparse
import os
import sys
import time

import strategy.strategy_functions as strategy_functions
from library.data_source_interface import TickerDataSource
from library.database_interface import Database, generate_unique_id
from library.exchange_interface import AlpacaInterface
from library.utils.file import parse_configs_file
from library.utils.job import Job
from library.utils.log import get_log_file_path, setup_log, log_configs


class TradeExecutor:

    def __init__(self, db, portfolio_id, exchange):
        self._db = db

        # Load latest portfolio data.
        portfolio_row = db.get_one_row('portfolios', 'id="{0}"'.format(portfolio_id))
        asset_rows = db.query_table('assets', 'portfolio_id="{0}"'.format(portfolio_row[0]))

        self.portfolio = {"id": portfolio_id,
                          "assets": {r[2]: int(r[3]) for r in asset_rows},
                          "capital": float(portfolio_row[2])}
        # This is the passed exchange object, currently has nothing to do with exchange_name.
        self.exchange = exchange
        self._exchange_name = str(portfolio_row[1])

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
        # TODO Sync assets too.
        capital = self.exchange.get_equity()
        if capital:
            self.portfolio['capital'] = capital
        else:
            raise Exception('Could not sync portfolio with exchange.')

    def meets_risk_profile(self, strategy, proposed_trade, risk_profile):
        strategy_risk_profile = risk_profile[strategy]
        signal, symbol, no_of_units, target_price = proposed_trade
        current_exposure = self.calculate_exposure(symbol)

        if 'max_exposure' in strategy_risk_profile:
            if signal == 'buy':
                potential_exposure = current_exposure + (no_of_units * target_price)
                if potential_exposure > strategy_risk_profile['max_exposure']:
                    return False
        # TODO Implement more risk checks.
        if 'min_liquidity' in risk_profile:
            return True
        return True

    def propose_trades(self, strategy, signals, risk_profile):
        trades = []
        for signal in signals:
            if signal.signal != 'hold':
                # Calculate number of units for trade.
                units = 1

                # Create trade tuple.
                trade = (signal.signal, signal.symbol, units, signal.target_value)

                # Check trade is valid.
                if self.meets_risk_profile(strategy, trade, risk_profile):
                    # Check symbol is in portfolio.
                    if signal.symbol not in self.portfolio['assets']:
                        raise Exception('Asset "{0}" not found in portfolio.'.format(signal.symbol))

                    # Check portfolio has sufficient capital.
                    if signal.signal == 'buy':
                        # Calculate required capital.
                        required_capital = units * signal.target_value

                        # Ensure portfolio's capital is up-to-date.
                        self.sync_portfolio_with_exchange()
                        if required_capital > float(self.portfolio['capital']):
                            raise Exception('Required capital has exceeded limits.')
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

            # trade = (data['symbol'], data['symbol'], int(data['filled_qty']), float(data['filled_avg_price']))

            if trade:
                # Update portfolio capital.
                change_in_capital = (int(data['filled_qty']) * float(data['filled_avg_price'])) * 1 if data['side'] == 'sell' else -1
                self.portfolio['capital'] += change_in_capital

                # Update portfolio assets.
                change_in_units = int(trade[1]) * 1 if data['side'] == 'buy' else -1
                self.portfolio['assets'][data['symbol']] += change_in_units

                # Add to processed trades list.
                processed_trades.append(trade)
            else:
                log.warning('Order {0} [{1} * {2}] failed. status: {3}'.format(order_id, data['qty'], data['symbol'], status))
        return processed_trades

    def _get_order_data(self, order_id):
        return [o for o in self.exchange.get_orders() if o['id'] == order_id][0]

    def update_portfolio_db(self, updated_by, ds):
        # Ensure capital is up-to-date with exchange.
        self.sync_portfolio_with_exchange()

        # Add new row for portfolio with updated capital.
        self._db.update_value('portfolios', 'capital', self.portfolio['capital'], 'id="{}"'.format(self.portfolio['id']))
        self._db.update_value('portfolios', 'updated_by', updated_by, 'id="{}"'.format(self.portfolio['id']))

        # Update assets.
        for asset in self.portfolio['assets']:
            units = self.portfolio['assets'][asset]
            self._db.update_value('assets', 'units', units, 'symbol="{}"'.format(asset))

        # Valuate portfolio and record in database.
        tickers = ds.request_tickers([a for a in self.portfolio['assets']])
        total_current_value_of_assets = sum([self.portfolio['assets'][asset] * float(tickers[asset])
                                             for asset in self.portfolio['assets']])
        portfolio_value = self.portfolio['capital'] + total_current_value_of_assets
        now = datetime.datetime.now()
        self._db.insert_row('historical_portfolio_valuations', [
            generate_unique_id(now),
            self.portfolio['id'],
            now.strftime('%Y%m%d%H%M%S'),
            portfolio_value
        ])


class SignalGenerator:

    def __init__(self, db, log, run_date, run_time, ds=None):
        self._db = db
        self._ds = ds if ds else None
        self._log = log
        self._run_date = run_date
        self._run_time = run_time

    def _evaluate_strategy(self, strategy_name):
        # Get strategy function and arguments.
        context = StrategyContext(self._db, strategy_name, self._run_date, self._run_time, self._ds)
        strategy_row = self._db.get_one_row('strategies', 'name="{0}"'.format(strategy_name))
        args = strategy_row[4]
        func = strategy_row[5]

        # Check method exists.
        if func not in dir(strategy_functions):
            raise Exception('Strategy function "{0}" not found.'.format(func))

        # Prepare function call.
        args = ['"{0}"'.format(a) for a in args.split(',')] if args else ''
        args_str = ','.join(args)
        try:
            signal = eval('strategy_functions.{0}(context,{1})'.format(func, args_str))
        except Exception as error:
            signal = error

        # Save signals to db or handle strategy errors.
        if not isinstance(signal, Signal):
            self._log.error('Error evaluating strategy "{0}": {1}'.format(strategy_name, signal))

        return signal

    def evaluate_strategies(self, strategy):
        # Evaluate strategy.
        return self._evaluate_strategy(strategy)


class StrategyContext:

    def __init__(self, db, strategy_name, run_date, run_time, ds=None):
        now = datetime.datetime.now()
        run_date = run_date if run_date else now.strftime('%Y%m%d')
        run_time = run_time if run_time else now.strftime('%H%M%S')
        self.now = datetime.datetime.strptime(run_date + run_time, '%Y%m%d%H%M%S')
        self.db = db
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
        # "target" because can always sell for more or buy for less I assume.
        self.target_value = 0
        # TODO Allow different order types.
        self.order_type = 'market'
        self.datetime = datetime.datetime.now()

    def __str__(self):
        # target_value_pp = '' if self.signal == 'hold' else ' @ {0}'.format(str(self.target_value))
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

        # If all the signals agree unify signal per symbol, else raise error for symbol (maybe allow manual override)
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
            raise Exception('Could not unify conflicting signals for "{0}": {1}'.format(symbol, conflicting_signals_str))

    # Return cleaned signals.
    return [signals_per_unique_symbol[signal] for signal in unique_symbols]


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
    parser.add_option('--dry_run', action="store_true", default=False)

    # Initiate script specific args.
    parser.add_option('-s', '--strategy', dest="strategy")
    parser.add_option('-d', '--data_source', dest="data_source")
    # Specify "simulate" or "execute" modes.
    parser.add_option('-m', '--mode', dest="mode")
    # Can be ran for any date or time, both default to now.
    #   these will help back testing, and can make the run_time precise and remove any lag in cron.
    parser.add_option('--run_date', dest="run_date", default=None)
    parser.add_option('--run_time', dest="run_time", default=None)

    options, args = parser.parse_args()
    return parse_configs_file({
        "app_name": app_name,
        "environment": options.environment.lower(),
        "root_path": options.root_path,
        "job_name": options.job_name,
        "script_name": str(os.path.basename(sys.argv[0])).split('.')[0],
        "dry_run": options.dry_run,

        # Parse script specific args.
        "data_source": options.data_source,
        "strategy": options.strategy.lower(),
        "mode": options.mode,
        "run_date": options.run_date,
        "run_time": options.run_time
    })


def main():
    # Setup configs.
    global configs
    configs = parse_cmdline_args('algo_trading_platform')

    # Setup logging.
    log_path = get_log_file_path(configs['logs_root_path'], configs['script_name'])
    log = setup_log(log_path, True if configs['environment'] == 'dev' else False)
    log_configs(configs, log)

    # Setup database.
    db = Database(configs['db_root_path'], 'algo_trading_platform', configs['environment'])
    db.log(log)

    # Initiate Job
    job = Job(configs, db)
    job.log(log)

    # Setup data source if one is specified in the args.
    ds_name = configs['data_source']
    ds = TickerDataSource(db, ds_name) if ds_name else None
    log.info('Initiated data source: {0}'.format(ds_name))

    # Evaluate strategy [Signals], just this section can be used to build a strategy function test tool.
    # Possibly pass in exchange here so can be used to get live data instead of ds (ds is good enough for MVP).
    job.update_status('Evaluating strategies')
    signal_generator = SignalGenerator(db, log, configs['run_date'], configs['run_time'], ds)
    # Only takes on strategy.
    signals = signal_generator.evaluate_strategies(configs['strategy'])

    job.update_status('Processing signals')
    # Check for conflicting signals [Signals].
    cleaned_signals = clean_signals(signals)

    if not cleaned_signals:
        # Script cannot go any further from this point, but should not error.
        # TODO add job terminator, and log as warning.
        raise Exception('No valid signals.')
        # pass
    log.info('Generated {0} valid signals.'.format(len(cleaned_signals)))
    for signal in cleaned_signals:
        log.info(str(signal))

    # Calculate risk profile {string(strategy name): float(risk value)}.
    risk_profile = generate_risk_profile(db, configs['strategy'])

    # Initiate exchange.
    if configs['mode'] == 'simulate':
        exchange = AlpacaInterface(configs['API_ID'], configs['API_SECRET_KEY'], simulator=True)
    elif configs['mode'] == 'execute':
        exchange = AlpacaInterface(configs['API_ID'], configs['API_SECRET_KEY'])
    else:
        # Script cannot go any further from this point.
        # TODO add (generic bad configs) job terminator.
        raise Exception('Mode "{0}" is not valid.'.format(configs['mode']))

    # Initiate trade executor.
    job.update_status('Proposing trades')
    portfolio_id = db.get_one_row('strategies', 'name="{0}"'.format(configs['strategy']))[3]
    trade_executor = TradeExecutor(db, portfolio_id, exchange)

    # Prepare trades.
    proposed_trades = trade_executor.propose_trades(configs['strategy'], cleaned_signals, risk_profile)

    # Execute trades.
    job.update_status('Executing trades')
    executed_order_ids = trade_executor.execute_trades(proposed_trades)

    # Process trades.
    job.update_status('Processing trades')
    processed_trades = trade_executor.process_executed_trades(executed_order_ids, log)
    trade_executor.update_portfolio_db(job.id, ds)

    # Log summary.
    log.info('Executed {0}/{1} trades successfully.'.format(len(processed_trades), len(executed_order_ids)))

    job.finished(log)


if __name__ == "__main__":
    sys.exit(main())

