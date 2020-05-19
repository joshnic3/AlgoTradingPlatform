import datetime
import optparse
import os
import sys
import time

from library.bootstrap import Constants
from library.interfaces.exchange import AlpacaInterface
from library.interfaces.sql_database import Database, generate_unique_id
from library.utilities.file import parse_configs_file
from library.utilities.job import Job
from library.utilities.log import get_log_file_path, setup_log, log_configs
from library.strategy import parse_strategy_from_xml, Signal, Portfolio


class TradeExecutor:

    def __init__(self, db, strategy, exchange):
        self._db = db

        self.strategy = strategy
        self.portfolio = self.strategy.portfolio
        self.exchange = exchange

    def _get_order_data(self, order_id):
        return [o for o in self.exchange.get_orders() if o['id'] == order_id][0]

    @staticmethod
    def _meets_risk_profile(portfolio, risk_profile):
        # Return True if current state of the potential portfolio meets the strategy's risk profile.

        # Make sure we only sell what we have.
        portfolio_meets_risk_profile = True
        negative_units = [portfolio.assets[a][Portfolio.SYMBOL] for a in portfolio.assets if portfolio.assets[a][Portfolio.UNITS] < 0]
        if negative_units:
            assets = ', '.join(negative_units) if len(negative_units) > 1 else negative_units[0]
            Constants.log.warning('Not enough units of {0} held for trade.'.format(assets))
            portfolio_meets_risk_profile = False

        # Enforce exposure limit
        if 'max_exposure' in risk_profile:
            exposure = sum([portfolio.assets[a][Portfolio.EXPOSURE] for a in portfolio.assets])
            exposure_overflow = exposure - float(risk_profile['max_exposure'])
            if exposure_overflow > 0:
                Constants.log.warning('Maximum exposure limit exceeded by {0}.'.format(abs(exposure_overflow)))
                portfolio_meets_risk_profile = False

        # if 'min_liquidity' in risk_profile:
        #     return True

        return portfolio_meets_risk_profile

    def generate_trades_from_signals(self, signals):
        # Generates trades from signals using the strategy's risk profile and and execution options.

        # Manage exposure.
        # Units to buy/sell could be varied to balance exposure.
        # Could sell units if exposure limit is exceeded
        # exposure_manager = ExposureManager(signals, strategy) if 'manage_exposure' in strategy.execution_options else None
        exposure_manager = None

        self.portfolio.sync_with_exchange(self.exchange)
        potential_portfolio = self.portfolio
        trades = []
        for signal in signals:
            if signal.signal != Signal.HOLD:
                # Decide how many units to trade using strategy options and portfolio data.
                if exposure_manager:
                    # units = exposure_manager.suggest_units_to_trade(signal)
                    units = 1
                else:
                    units = 1

                # Make potential portfolio changes for sell order.
                # TODO may need to consider exchange commissions here.
                if signal.signal == Signal.SELL:
                    potential_portfolio.cash += units * signal.target_value
                    potential_portfolio.assets[signal.symbol][Portfolio.UNITS] -= units

                # Make potential portfolio changes for buy order.
                if signal.signal == Signal.BUY:
                    potential_portfolio.cash -= units * signal.target_value
                    potential_portfolio.assets[signal.symbol][Portfolio.UNITS] += units

                # Calculate total potential exposure.
                potential_exposure = self.portfolio.calculate_exposure(signal.symbol, potential_portfolio)
                potential_portfolio.assets[signal.symbol][Portfolio.EXPOSURE] = potential_exposure

                # Only append trade if current state of the potential portfolio meets the strategy's risk profile.
                if self._meets_risk_profile(potential_portfolio, self.strategy.risk_profile):
                    trades.append((signal.signal, signal.symbol, units, signal.target_value))

        if trades:
            Constants.log.info('Generated {0} trade(s) from {1} signals.'.format(len(trades), len(signals)))
        return trades

    def execute_trades(self, requested_trades):
        # Return actual achieved trades, Not all trades will be fulfilled.
        executed_trade_ids = []
        for trade in requested_trades:
            signal, symbol, units, target_value = trade
            if signal == Signal.SELL:
                executed_trade_ids.append(self.exchange.ask(symbol, units))
            if signal == Signal.BUY:
                executed_trade_ids.append(self.exchange.bid(symbol, units))
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

            # Create order tuple with trade results.
            if status == 'filled':
                trade = (data['symbol'], int(data['filled_qty']), float(data['filled_avg_price']))

                # Update portfolio capital.
                change_in_capital = (int(data['filled_qty']) * float(data['filled_avg_price'])) * 1 if data['side'] == Signal.SELL else -1
                self.portfolio[Portfolio.CASH] += change_in_capital

                # Update portfolio assets.
                change_in_units = int(trade[1]) * 1 if data['side'] == Signal.BUY else -1
                self.portfolio.assets[data[Portfolio.SYMBOL]][Portfolio.UNITS] += change_in_units

                # Add to processed trades list.
                processed_trades.append(trade)
            else:
                log.warning('Order {0} [{1} * {2}] failed. status: {3}'.format(order_id, data['qty'], data['symbol'], status))
        return processed_trades

    def update_portfolio_db(self, append_to_historical_values=True):
        self.portfolio.sync_with_exchange(self.exchange)
        self.portfolio.update_db()

        if append_to_historical_values:
            now = datetime.datetime.now()
            self._db.insert_row('historical_portfolio_valuations', [
                generate_unique_id(now),
                self.portfolio.id,
                now.strftime('%Y%m%d%H%M%S'),
                self.portfolio.valuate()
            ])


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
    Constants.log.info("Strategy portfolio: {0}".format(strategy.portfolio.id))
    db.update_value('strategies', 'updated_by', job.id, 'name="{}"'.format(strategy.name.lower()))

    # Evaluate strategy,
    signals = strategy.generate_signals()

    if not signals:
        # Script cannot go any further from this point, but should not error.
        job.finished(condition='no valid signals')
        return 2

    # Log signals.
    Constants.log.info('Generated {0} valid signal(s): {1}.'.format(len(signals), ', '.join([str(s) for s in signals])))

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
    trade_executor = TradeExecutor(db, strategy, exchange)

    # Prepare trades.
    proposed_trades = trade_executor.generate_trades_from_signals(signals)
    if not proposed_trades:
        # Script cannot go any further from this point, but should not error.
        job.finished(condition='no proposed trades')
        return 2

    # Execute trades.
    job.update_phase('Executing_trades')
    executed_order_ids = trade_executor.execute_trades(proposed_trades)

    # Process trades.
    job.update_phase('Processing_trades')
    processed_trades = trade_executor.process_executed_trades(executed_order_ids, Constants.log)

    Constants.log.info('Updated portfolio in database.')
    trade_executor.update_portfolio_db()

    # Log summary.
    Constants.log.info('Executed {0}/{1} trades successfully.'.format(len(processed_trades), len(executed_order_ids)))
    job.finished()


if __name__ == "__main__":
    sys.exit(main())

