import datetime
import json
import optparse
import os
import sys
import time

import requests

import strategy.strategy_functions as strategy_functions
from library.db_utils import Database
from library.ds_utils import TickerDataSource
from library.file_utils import parse_configs_file
from library.job_utils import Job, get_job_phase_breakdown
from library.log_utils import get_log_file_path, setup_log, log_configs


class AlpacaInterface:
    # TODO Handle response errors.

    def __init__(self, key_id, secret_key, simulator=False):
        if simulator:
            base_url = 'https://paper-api.alpaca.markets'

        self.headers = {'APCA-API-KEY-ID': key_id, 'APCA-API-SECRET-KEY': secret_key}
        self.api = {
            'ACCOUNT': '{0}/v2/account'.format(base_url),
            'ORDERS': '{0}/v2/orders'.format(base_url),
            'POSITIONS': '{0}/v2/positions'.format(base_url),
            'CLOCK': '{0}/v2/clock'.format(base_url)
        }

        if not self.is_exchange_open():
            # TODO also check if authorised here and raise appropriate exception.
            raise Exception('Exchange is closed.')

    def is_exchange_open(self):
        results = requests.get(self.api['CLOCK'], headers=self.headers)
        data = json.loads(results.content.decode('utf-8'))
        return data['is_open']

    def get_orders(self):
        data = {"status": "all"}
        results = requests.get(self.api['ORDERS'], params=data, headers=self.headers, )
        return json.loads(results.content.decode('utf-8'))

    def get_position(self, symbol, key=None):
        results = requests.get('{}/{}'.format(self.api['POSITIONS'], symbol), headers=self.headers)
        data = json.loads(results.content.decode('utf-8'))
        if 'code' in data:
            return 0
        if key in data:
            return data[key]
        return data

    def ask(self, symbol, units):
        results = self._create_order(symbol, units, 'sell')
        if not results['id']:
            return None
        return results['id']

    def bid(self, symbol, units):
        results = self._create_order(symbol, units, 'buy')
        if not results['id']:
            return None
        return results['id']

    def _create_order(self, symbol, units, side):
        # Assuming all orders at this point are valid.
        # Will offer limit orders in the future.
        data = {"symbol": symbol, "qty": units, "side": side, "type": "market", "time_in_force": "gtc"}
        # Ensure can sell if required.
        if side == 'sell' and self.get_position(symbol, 'qty') is None:
            raise Exception('There is no "{0}" in portfolio.'.format(symbol))
        results = requests.post(self.api['ORDERS'], json=data, headers=self.headers)
        return json.loads(results.content.decode('utf-8'))


class TradeExecutor:

    def __init__(self, db, portfolio_name, exchange):
        self._db = db

        # Read portfolio details from database.
        pf_id, pf_name, exchange_name, capital, updated_by = db.get_one_row('portfolios', 'name="{0}"'.format(portfolio_name))
        results = db.query_table('assets', 'portfolio_id="{0}"'.format(pf_id))
        self.portfolio = {"id": pf_id,
                          "assets": {r[2]: int(r[3]) for r in results},
                          "capital": float(capital)}
        # This is the passed exchange object, currently has nothing to do with exchange_name.
        self.exchange = exchange

    def calculate_exposure(self, symbol):
        # Assume exposure == maximum possible loss from current position.
        # Get this from exchange?
        data = self.exchange.get_position(symbol)
        if data:
            units = int(data['qty'])
            current_value = float(data['current_price'])
            return units * current_value
        return 0.0

    def meets_risk_profile(self, strategy, proposed_trade, risk_profile):
        strategy_risk_profile = risk_profile[strategy]
        signal, symbol, no_of_units, target_price = proposed_trade
        if 'max_exposure' in strategy_risk_profile:
            if signal == 'buy':
                potential_exposure = self.calculate_exposure(symbol) + (no_of_units * target_price)
            else:
                potential_exposure = self.calculate_exposure(symbol) - (no_of_units * target_price)
            if potential_exposure > strategy_risk_profile['max_exposure']:
                return False
        # TODO Implement more risk checks.
        if 'min_liquidity' in risk_profile:
            return True
        return True

    def propose_trades(self, strategies_list, signals, risk_profile):
        trades = []
        for strategy in strategies_list:
            # Get required data from database.
            strategy_symbol = self._db.get_one_row('strategies', 'name="{0}"'.format(strategy))[3]
            signal = [s for s in signals if s.symbol == strategy_symbol][0]

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
                    required_capital = units * signal.target_value
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
                multiplier = 1 if data['side'] == 'sell' else -1
                change_in_capital = (int(data['filled_qty']) * float(data['filled_avg_price'])) * multiplier
                self.portfolio['capital'] += change_in_capital

                # Update portfolio assets.
                change_in_units = int(trade[1]) * multiplier
                self.portfolio['assets'][data['symbol']] += change_in_units

                # Add to processed trades list.
                processed_trades.append(trade)
            else:
                log.warning('Order {0} [{1} * {2}] failed. status: {3}'.format(order_id, data['qty'], data['symbol'], status))
        return processed_trades

    def _get_order_data(self, order_id):
        return [o for o in self.exchange.get_orders() if o['id'] == order_id][0]

    def update_portfolio_db(self, updated_by):
        # TODO Portfolio and Asset tables should have new rows added for updates, with datetime and job id
        # Update capital.
        self._db.update_value('portfolios', 'capital', self.portfolio['capital'], 'id="{}"'.format(self.portfolio['id']))

        # Update job id.
        self._db.update_value('portfolios', 'updated_by', updated_by, 'id="{}"'.format(self.portfolio['id']))

        # Update assets.
        assets = self.portfolio['assets']
        for asset in assets:
            units = self.portfolio['assets'][asset]
            self._db.update_value('assets', 'units', units, 'symbol="{}"'.format(asset))


class SignalGenerator:

    def __init__(self, db, log, run_date, run_time, ds=None):
        self._db = db
        self._ds = ds if ds else None
        self._log = log
        self._run_date = run_date
        self._run_time = run_time

    def _evaluate_strategy(self, strategy_name):
        # Get strategy function and arguments.
        context = StrategyContext(self._db, self._run_date, self._run_time, self._ds)
        strategy_row = self._db.get_one_row('strategies', 'name="{0}"'.format(strategy_name))
        args = strategy_row[3]
        func = strategy_row[4]

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
        if not signal:
            self._log.error('Error evaluating strategy "{0}": {1}'.format(strategy_name, signal))

        return signal

    def evaluate_strategies(self, strategies_list):
        # Evaluate strategy.
        #   Might want to evaluate concurrently?
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


def generate_risk_profile(db, strategies_list, risk_appetite=1.0):
    # Returns risk profile, dict of factor: values.
    risk_profile = {}
    for strategy in strategies_list:
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
    sg = SignalGenerator(db, log, configs['run_date'], configs['run_time'], ds)
    strategies_list = configs['strategy'].split(',')
    signals = sg.evaluate_strategies(strategies_list)

    job.update_status('Processing signals')
    # Check for conflicting signals [Signals].
    cleaned_signals = clean_signals(signals)

    if not cleaned_signals:
        raise Exception('No valid signals.')

    # Calculate risk profile {string(strategy name): float(risk value)}.
    risk_profile = generate_risk_profile(db, strategies_list)

    # Initiate exchange.
    if configs['mode'] == 'simulate':
        exchange = AlpacaInterface(configs['API_ID'], configs['API_SECRET_KEY'], simulator=True)
    elif configs['mode'] == 'execute':
        exchange = AlpacaInterface(configs['API_ID'], configs['API_SECRET_KEY'])
    else:
        raise Exception('Mode "{0}" is not valid.'.format(configs['mode']))

    # Initiate trade executor.
    job.update_status('Proposing trades')
    trade_executor = TradeExecutor(db, 'test_portfolio', exchange)

    # Prepare trades.
    proposed_trades = trade_executor.propose_trades(strategies_list, cleaned_signals, risk_profile)

    # Execute trades.
    job.update_status('Executing trades')
    executed_order_ids = trade_executor.execute_trades(proposed_trades)

    # Process trades.
    job.update_status('Processing trades')
    processed_trades = trade_executor.process_executed_trades(executed_order_ids, log)
    trade_executor.update_portfolio_db(job.id)

    # Log summary.
    log.info('{0}/{1} trades successful.'.format(len(processed_trades), len(executed_order_ids)))

    job.finished(log)
    print(get_job_phase_breakdown(db, job.id))


if __name__ == "__main__":
    sys.exit(main())
