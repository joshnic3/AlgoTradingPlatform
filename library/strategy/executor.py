import os

from library.bootstrap import Constants
from library.interfaces.exchange import SimulatedExchangeInterface, AlpacaInterface
from library.strategy.bread_crumbs import BreadCrumbs
from library.strategy.reporting import export_strategy_bread_crumbs_to_csv
from library.trade_executor import TradeExecutor


class StrategyExecutor:

    def __init__(self, strategy_object, suppress_log=False, suppress_trades=False,
                 simulate_exchange=False, job_object=False):
        self._suppress_log = suppress_log
        self._job_object = job_object
        self._suppress_trades = suppress_trades

        self.strategy = strategy_object
        if simulate_exchange:
            # TODO Getting issues with consistency again.
            exchange = SimulatedExchangeInterface(self.strategy, 100000.00)
        else:
            exchange = AlpacaInterface(Constants.configs[AlpacaInterface.API_ID],
                                       Constants.configs[AlpacaInterface.API_SECRET_KEY], paper=True)
        self.trade_executor = TradeExecutor(self.strategy, exchange)
        self.finish_condition = None

    def _update_phase(self, phase_string):
        if self._job_object:
            self._job_object.update_phase(phase_string)

    def _log(self, log_string):
        if not self._suppress_log:
            Constants.log.info(log_string)

    def run(self, run_datetime_override=None):
        # Override strategy run datetime if required.
        if run_datetime_override:
            self.strategy.run_datetime = run_datetime_override

        # Generate signals.
        signals = self.strategy.generate_signals()

        # Trade if we want to.
        trades = None
        if signals:
            # Propose trades.
            self._log('Generated {0} valid signal(s): {1}.'.format(len(signals), ', '.join([str(s) for s in signals])))
            self._update_phase('proposing trades')
            proposed_trades = self.trade_executor.generate_trades_from_signals(signals)

            if proposed_trades:
                # Execute any trades if withhold flag is not set.
                if not self._suppress_trades:
                    # Execute and process trades.
                    self._update_phase('executing trades')
                    executed_order_ids = self.trade_executor.execute_trades(proposed_trades)
                    self._update_phase('processing trades')
                    trades = self.trade_executor.process_executed_trades(executed_order_ids,
                                                                         suppress_log=self._suppress_log)
                    self._log('Executed {0}/{1} trades successfully.'.format(len(trades), len(executed_order_ids)))
                    self.strategy.bread_crumbs.drop(self.strategy.run_datetime, BreadCrumbs.TRADES,
                                                    [BreadCrumbs.SEPARATOR.join([str(e) for e in t]) for t in trades]
                                                    if trades else None)

        # Now all the work is done, update portfolio to database.
        self.trade_executor.update_portfolio_db()

        # Set finish condition.
        if not signals:
            self.finish_condition = 'no signals'
        elif trades and self._suppress_trades:
            self._log('Produced {0} trade(s).'.format(len(proposed_trades)))
            self.finish_condition = 'suppressed trades'
        elif not trades:
            self.finish_condition = 'no trades'
        else:
            self.finish_condition = None

        # Drop valuation bread crumb.
        self.strategy.bread_crumbs.drop(self.strategy.run_datetime, BreadCrumbs.VALUATION,
                                        self.strategy.portfolio.valuate())

        # Return final valuation.
        return self.trade_executor.portfolio.valuate()

    def generate_strategy_report(self):
        sub_directory = 'strategies'

        # Generate file name.
        file_name = '{}_{}_strategy_report.csv'.format(Constants.run_time.strftime(
            Constants.DATETIME_FORMAT[:6]), self.strategy.name)

        # Ensure regression sub directory exists.
        strategy_reports_directory = os.path.join(Constants.reports_path, sub_directory)
        if not os.path.isdir(strategy_reports_directory):
            os.mkdir(strategy_reports_directory)

        # Generate csv report.
        export_strategy_bread_crumbs_to_csv(self.strategy, os.path.join(strategy_reports_directory, file_name))


