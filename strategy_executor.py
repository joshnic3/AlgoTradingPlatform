import datetime
import optparse
import os
import sys
import time

from library.bootstrap import Constants
from library.exposure_manager import ExposureManager
from library.interfaces.exchange import AlpacaInterface as Alpaca
from library.interfaces.sql_database import Database, generate_unique_id
from library.portfolio import Portfolio
from library.strategy import parse_strategy_from_xml, Signal
from library.utilities.file import parse_configs_file
from library.utilities.job import Job
from library.utilities.log import get_log_file_path, setup_log, log_configs


class TradeExecutor:

    def __init__(self, db, strategy, exchange):
        self._db = db
        self._default_no_of_units = 1

        self.strategy = strategy
        self.risk_profile = strategy.risk_profile
        self.portfolio = self.strategy.portfolio
        self.exchange = exchange

    def generate_trades_from_signals(self, signals):
        # Generates trades from signals using the strategy's risk profile and and execution options.

        # Manage exposure if specified in stratgey execution options.
        if 'manage_exposure' in self.strategy.execution_options:
            exposure_manager = ExposureManager(self.strategy, default_units=self._default_no_of_units)
        else:
            exposure_manager = None

        self.portfolio.sync_with_exchange(self.exchange)
        potential_portfolio = self.portfolio
        trades = []
        for signal in signals:
            if signal.signal != Signal.HOLD:
                # Decide how many units to trade using strategy options and portfolio data.
                units = exposure_manager.units_to_trade(signal) if exposure_manager else self._default_no_of_units

                # Make potential portfolio changes for sell order.
                # TODO May need to consider exchange commissions here.
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
                if self.risk_profile.assess_portfolio(potential_portfolio):
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
            data = self.exchange.get_order_data(order_id)
            status = data[Alpaca.STATUS]

            # Wait for order to fill.
            while status == Alpaca.NEW_ORDER or status == Alpaca.PARTIALLY_FILLED_ORDER:
                time.sleep(0.5)
                data = self.exchange.get_order_data(order_id)
                status = data[Alpaca.STATUS]

            # Create order tuple with trade results.
            if status == Alpaca.FILLED_ORDER:
                trade = (data[Portfolio.SYMBOL], int(data[Alpaca.FILLED_UNITS]), float(data[Alpaca.FILLED_MEAN_PRICE]))

                # Update portfolio capital.
                change_in_capital = (int(data[Alpaca.FILLED_UNITS]) * float(data[Alpaca.FILLED_MEAN_PRICE])) * 1 \
                    if data[Alpaca.ORDER_SIDE] == Signal.SELL else -1
                self.portfolio.cash += change_in_capital

                # Update portfolio assets.
                change_in_units = int(trade[1]) * 1 if data[Alpaca.ORDER_SIDE] == Signal.BUY else -1
                self.portfolio.assets[data[Portfolio.SYMBOL]][Portfolio.UNITS] += change_in_units

                # Add to processed trades list.
                processed_trades.append(trade)
            else:
                log.warning('Order {0} [{1} * {2}] failed. status: {3}'
                            .format(order_id, data[Alpaca.UNITS], data[Alpaca.SYMBOL], status))
        return processed_trades

    def update_portfolio_db(self, append_to_historical_values=True):
        self.portfolio.sync_with_exchange(self.exchange)
        self.portfolio.update_db()

        # Record run value.
        if append_to_historical_values:
            now = datetime.datetime.now()
            self._db.insert_row('historical_portfolio_valuations', [
                generate_unique_id(now),
                self.portfolio.id,
                now.strftime(Constants.date_time_format),
                self.portfolio.valuate()
            ])
        Constants.log.info('Updated portfolio and recorded current valuation.')


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
    db = Database()
    db.log()

    # Initiate Job
    job = Job(log_path)
    job.log()

    # Parse strategy xml.
    strategy = parse_strategy_from_xml(Constants.configs['xml_path'], return_object=True, db=db)
    Constants.log.info("Strategy portfolio: {0}".format(strategy.portfolio.id))
    db.update_value('strategies', 'updated_by', job.id, 'name="{}"'.format(strategy.name.lower()))

    # Evaluate strategy,
    signals = strategy.generate_signals()

    if signals is None:
        # There was a calculation error. This is fatal.
        job.finished(condition='calculation error', status=Job.FAILED)
        return Job.FAILED

    if not signals:
        # Script cannot go any further from this point, but should not error.
        job.finished(condition='no signals')
        return Job.WARNINGS

    # Log signals.
    Constants.log.info('Generated {0} valid signal(s): {1}.'.format(len(signals), ', '.join([str(s) for s in signals])))

    # Initiate exchange.
    if Constants.configs['mode'] == 'simulate':
        exchange = Alpaca(Constants.configs['API_ID'], Constants.configs['API_SECRET_KEY'], simulator=True)
    elif Constants.configs['mode'] == 'execute':
        exchange = Alpaca(Constants.configs['API_ID'], Constants.configs['API_SECRET_KEY'])
    else:
        # Script cannot go any further from this point.
        raise Exception('Mode "{0}" is not valid.'.format(Constants.configs['mode']))
    if not exchange.is_exchange_open():
        # Script cannot go any further from this point, but should not error.
        job.finished(condition='exchange is closed', status=Job.WARNINGS)
        return Job.WARNINGS

    # Initiate trade executor.
    job.update_phase('Proposing_trades')
    trade_executor = TradeExecutor(db, strategy, exchange)

    # Prepare trades.
    proposed_trades = trade_executor.generate_trades_from_signals(signals)
    if not proposed_trades:
        # Script cannot go any further from this point, but should not error. Should still update porfolio though.
        trade_executor.update_portfolio_db()
        job.finished(condition='no proposed trades')
        return Job.WARNINGS

    # Execute trades.
    job.update_phase('Executing_trades')
    executed_order_ids = trade_executor.execute_trades(proposed_trades)

    # Process trades.
    job.update_phase('Processing_trades')
    processed_trades = trade_executor.process_executed_trades(executed_order_ids, Constants.log)
    trade_executor.update_portfolio_db()

    # Log summary.
    Constants.log.info('Executed {0}/{1} trades successfully.'.format(len(processed_trades), len(executed_order_ids)))
    job.finished()
    return Job.SUCCESSFUL


if __name__ == "__main__":
    sys.exit(main())

