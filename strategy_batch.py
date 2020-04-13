import sys
import datetime
import optparse
import os

import strategies.strategies as strat_file
from library.db_interface import Database
from library.data_source_utils import TickerDataSource
from library.file_utils import parse_configs_file
from library.log_utils import get_log_file_path, setup_log, log_configs, log_hr
from library.job_utils import Job


# TODO Implement Exchange.
class Exchange:

    def __init__(self):
        pass


# TODO Implement ExchangeSimulator.
class ExchangeSimulator(Exchange):

    def __init__(self, out_file_path):
        Exchange.__init__(self)
        self._out_path = out_file_path


# TODO Implement ExchangeInterface.
class ExchangeInterface(Exchange):

    def __init__(self):
        Exchange.__init__(self)


# TODO Implement TradeExecutor.
class TradeExecutor:

    def __init__(self, db, exchange, signals, risk_profile):
        self._db = db
        self._exchange = exchange
        self._signals = signals
        self._risk_profile = risk_profile

    def execute_trades(self):
        pass


class SignalGenerator:

    def __init__(self, db, log, run_date, run_time, ds=None):
        self._db = db
        self._ds = ds if ds else None
        self._log = log
        self._run_date = run_date
        self._run_time = run_time

    def _evaluate_strategy(self, strategy_name):
        context = StrategyContext(self._db, self._run_date, self._run_time, self._ds)
        strategy_row = self._db.query_table('strategies', 'name="{0}"'.format(strategy_name))[0]
        args = strategy_row[2]
        method = strategy_row[3]
        args = ['"{0}"'.format(a) for a in args.split(',')] if args else ''
        args_str = ','.join(args)
        try:
            signal = eval('strat_file.{0}(context,{1})'.format(method, args_str))
        except Exception as error:
            signal = error

        # Save signals to db or handle strategy errors.
        if not signal:
            self._log.error('Error evaluating strategy "{0}": {1}'.format(strategy_name, signal))

        return signal

    def evaluate_strategies(self, strategies_list):
        # Evaluate strategies.
        signals = [self._evaluate_strategy(s) for s in strategies_list]
        return signals


class StrategyContext:

    def __init__(self, db, run_date, run_time, ds=None):
        now = datetime.datetime.now()
        run_date = run_date if run_date else now.strftime('%Y%m%d')
        run_time = run_time if run_time else now.strftime('%H%M%S')
        self.now = datetime.datetime.strptime(run_date + run_time, '%Y%m%d%H%M%S')
        self.db = db
        self.ds = ds if ds else None
        self.signal = Signal(0)


class Signal:

    def __init__(self, signal_id):
        self.id = signal_id
        self.symbol = None
        self.signal = None
        # "target" because can always sell for more or buy for less I assume.
        self.target_value = None
        self.datetime = datetime.datetime.now()

    def __str__(self):
        target_value_pp = '' if self.signal == 'hold' else ' @ {0}'.format(str(self.target_value))
        return '[{0} {1}{2}]'.format(self.signal, self.symbol, target_value_pp)

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

    # TODO Implement Signal save_to_db.
    def save_to_db(self, db, log):
        if log:
            log.info('Saved signal: {}'.format(self.__str__()))


def clean_signals(dirty_signals):
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


def generate_risk_profile(db, strategies_list, risk_multiplier=1.0):
    # Returns risk value for each strategy where, risk value is the maximum accepted loss (assuming USD).
    risk_profile = {}
    for strategy in strategies_list:
        condition = 'name="{0}"'.format(strategy)
        strategy_risk_value = float(db.get_one_row('strategies', condition)[4])
        risk_profile[strategy] = strategy_risk_value * risk_multiplier
    return risk_profile


def parse_cmdline_args(app_name):
    parser = optparse.OptionParser()
    parser.add_option('-e', '--environment', dest="environment")
    parser.add_option('-r', '--root_path', dest="root_path")
    parser.add_option('-j', '--job_name', dest="job_name", default=None)
    parser.add_option('--dry_run', action="store_true", default=False)

    # Initiate script specific args.
    parser.add_option('-s', '--strategies', dest="strategies")
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
        "strategies": options.strategies.lower(),
        "mode": options.mode,
        "run_date": options.run_date,
        "run_time": options.run_time
    })


def main():
    # Setup configs.
    global configs
    configs = parse_cmdline_args('algo_trading_platform')
    # configs = parse_configs_file(cmdline_args)

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
    ds = TickerDataSource(configs['data_source'], configs['db_root_path'], configs['environment']) if configs['data_source'] else None

    # Evaluate strategies [Signals], just this section can be used to build a strategy function test tool.
    sg = SignalGenerator(db, log, configs['run_date'], configs['run_time'], ds)
    strategies_list = configs['strategies'].split(',')
    signals = sg.evaluate_strategies(strategies_list)

    # Check for conflicting signals [Signals].
    cleaned_signals = clean_signals(signals)
    for signal in cleaned_signals:
        signal.save_to_db(db, log)

    # Calculate risk profile {string(strategy name): float(risk value)}.
    risk_profile = generate_risk_profile(db, strategies_list)

    # Initiate trade executor.
    if configs['mode'] == 'simulate':
        exchange = ExchangeSimulator('trade_requests.csv')
    elif configs['mode'] == 'execute':
        exchange = ExchangeInterface()
    else:
        raise Exception('Mode "{0}" is not valid.'.format(configs['mode']))

    # Execute trades, should only need db on TradeExecutor level.
    # TODO Signals need to know which strategies they use, maybe add strat_id to signals (feels the wrong way round).
    te = TradeExecutor(db, exchange, cleaned_signals, risk_profile)
    te.execute_trades()

    job.finished(log)


if __name__ == "__main__":
    sys.exit(main())
