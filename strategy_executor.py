import datetime
import optparse
import os
import sys
import time

from library.bootstrap import Constants
from library.interfaces.exchange import AlpacaInterface
from library.interfaces.market_data import TickerDataSource
from library.interfaces.sql_database import Database, generate_unique_id
from library.utilities.file import parse_configs_file
from library.utilities.job import Job
from library.utilities.log import get_log_file_path, setup_log, log_configs
from library.utilities.strategy import parse_strategy_from_xml


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

    @staticmethod
    def _meets_risk_profile(portfolio, risk_profile):
        # Make sure we only sell what we have.
        portfolio_meets_risk_profile = True
        negative_units = [portfolio['assets'][a]['symbol'] for a in portfolio['assets'] if portfolio['assets'][a]['units'] < 0]
        if negative_units:
            Constants.log.warning('Not enough units of {0} held for trade.'.format(', '.join(negative_units)))
            portfolio_meets_risk_profile = False

        # Enforce exposure limit
        if 'max_exposure' in risk_profile:
            exposure = sum([portfolio['assets'][a]['current_exposure'] for a in portfolio['assets']])
            exposure_overflow = exposure - float(risk_profile['max_exposure'])
            if exposure_overflow > 0:
                Constants.log.warning('Maximum exposure limit exceeded by {0}.'.format(abs(exposure_overflow)))
                portfolio_meets_risk_profile = False

        # if 'min_liquidity' in risk_profile:
        #     return True

        return portfolio_meets_risk_profile

    def calculate_exposure(self, symbol, portfolio=None):
        # Assume exposure == maximum possible loss from current position.

        portfolio = portfolio if portfolio else self.portfolio
        data = self.exchange.get_position(symbol)
        if data:
            units = portfolio['assets'][symbol]['units']
            current_value = float(data['current_price'])
            return units * current_value
        return 0.0

    def sync_portfolio_with_exchange(self):
        # Sync weighted cash value for strategy portfolio.
        cash = self.exchange.get_cash()
        if cash:
            self.portfolio['cash'] = cash
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
            asset['current_exposure'] = self.calculate_exposure(asset['symbol'])

    def generate_trades_from_signals(self, signals, strategy):
        # Generates trades from signals using the strategy's risk profile and and execution options.

        self.sync_portfolio_with_exchange()
        potential_portfolio = self.portfolio
        trades = []
        for signal in signals:
            if signal.signal != 'hold':
                # Decide how many units to trade using strategy options and portfolio data.
                if 'auto_exposure' in strategy.execution_options:
                    # Units to buy/sell could be varied to balance exposure.
                    # Could sell units if exposure limit is exceeded
                    pass
                else:
                    units = 1

                # Make potential portfolio changes for sell order.
                # TODO may need to consider exchange commissions here.
                if signal.signal == 'sell':
                    potential_portfolio['cash'] += units * signal.target_value
                    potential_portfolio['assets'][signal.symbol]['units'] -= units

                # Make potential portfolio changes for buy order.
                if signal.signal == 'buy':
                    potential_portfolio['cash'] -= units * signal.target_value
                    potential_portfolio['assets'][signal.symbol]['units'] += units

                # Calculate total potential exposure.
                potential_exposure = self.calculate_exposure(signal.symbol, potential_portfolio)
                potential_portfolio['assets'][signal.symbol]['current_exposure'] = potential_exposure

                # Only append trade if current state of the potential meets the strategy's risk profile.
                if self._meets_risk_profile(potential_portfolio, strategy.risk_profile):
                    trades.append((signal.signal, signal.symbol, units, signal.target_value))

        if trades:
            Constants.log.info('{0} trade(s) passed risk checks.'.format(len(trades)))
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
    job = Job(log_path)
    job.log()

    # Parse strategy xml.
    strategy = parse_strategy_from_xml(Constants.configs['xml_path'], return_object=True)
    strategy.portfolio = db.get_one_row('strategies', 'name="{0}"'.format(strategy.name.lower()))[2]
    db.update_value('strategies', 'updated_by', job.id, 'name="{}"'.format(strategy.name.lower()))

    # Evaluate strategy,
    signals = strategy.generate_signals()

    if not signals:
        # Script cannot go any further from this point, but should not error.
        job.finished(condition='no valid signals')
        return 2

    # Log signals.
    Constants.log.info('Generated {0} valid signal(s).'.format(len(signals)))
    Constants.log.info(', '.join([str(s) for s in signals]))

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
    trade_executor = TradeExecutor(db, strategy.portfolio, exchange)

    # Prepare trades.
    proposed_trades = trade_executor.generate_trades_from_signals(signals, strategy)

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

