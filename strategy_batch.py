import sys
import datetime

import strategies.strategies as strat_file
from library.db_interface import Database
from library.data_source_utils import TickerDataSource


class StrategyContext:

    def __init__(self, db_root_path, environment, data_source, run_date, run_time):
        db_name = 'algo_trading_platform'
        now = datetime.datetime.now()
        run_date = run_date if run_date else now.strftime('%Y%m%d')
        run_time = run_time if run_time else now.strftime('%H%M%S')
        self.now = datetime.datetime.strptime(run_date + run_time, '%Y%m%d%H%M%S')
        self.db = Database(db_root_path, db_name, environment)
        self.ds = TickerDataSource(data_source, db_root_path, environment)
        self.signal = Signal(0)


class Strategy:

    def __init__(self, context, name):
        self._context = context
        self._method = name.lower()
        args = self._context.db.query_table('strategies', 'name="{0}"'.format(self._method))[0][2]
        self._args = ['"{0}"'.format(a) for a in args.split(',')] if args else ''

    def __str__(self):
        return str(self._method)

    def evaluate(self):
        context = self._context
        args_str = ','.join(self._args)
        try:
            return eval('strat_file.{0}(context,{1})'.format(self._method, args_str))
        except Exception as error:
            return error


class Signal:

    def __init__(self, signal_id):
        self.id = signal_id
        self.symbol = None
        self.signal = None
        # "target" because can always sell for more or buy for less I assume.
        self.target_value = None
        self.datetime = datetime.datetime.now()

    def __str__(self):
        target_value_pp = ' @ {2}'.format(self.target_value) if self.target_value else ''
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

    def save_to_db(self, db):
        pass


def main():
    # Can be ran for any datetime, defaults to now.
    # can also specify simulation or execution modes
    # these will help back testing.

    # Parse configs.
    db_root_path = '/Users/joshnicholls/PycharmProjects/algo_trading_platform/drive/data'
    environment = 'dev'
    data_source = 'FML'
    strategies_to_run = ['basic', 'pairs']
    run_date = None
    run_time = None
    mode = 'simulate'

    # Prepare strategies.
    context = StrategyContext(db_root_path, environment, data_source, run_date, run_time)
    strategies = [Strategy(context, s) for s in strategies_to_run]

    # Generate and save signals to database.
    db = context.db
    for strategy in strategies:
        signal = strategy.evaluate()
        if isinstance(signal, Signal):
            signal.save_to_db(db)
            print('Saved signal: {}'.format(signal))
        else:
            print('Error evaluating strategy "{0}": {1}'.format(strategy, signal))

    # print(signals)

    # TODO implement trade executor/simulator.
    # These two will take exactly same inputs, both will output the same.
    # Simulation can out more analytics if needed.
    # could use same parent class but use different trade interface.
    # Exchange Class could have simulation subclass.
    if mode == 'simulate':
        pass
    if mode == 'execute':
        pass


if __name__ == "__main__":
    sys.exit(main())
