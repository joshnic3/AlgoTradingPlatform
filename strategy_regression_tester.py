import csv
import datetime
import os
import shutil
import sys

import pytz

from library.bootstrap import Constants, log_hr
from library.data_loader import BreadCrumbsDataLoader
from library.interfaces.exchange import SimulatedExchangeInterface
from library.interfaces.sql_database import initiate_database, generate_unique_id
from library.strategy.bread_crumbs import BreadCrumb
from library.strategy.bread_crumbs import set_bread_crumb
from library.strategy.strategy import parse_strategy_from_xml, parse_strategy_setup_from_xml, Strategy, RiskProfile
from library.trade_executor import TradeExecutor
from library.utilities.onboarding import add_portfolio, add_strategy, add_assets
from library.utilities.job import Job


def format_datetime_str(datetime_string):
    if datetime_string is None:
        return None
    date_time = datetime.datetime.strptime(datetime_string, Constants.DATETIME_FORMAT)
    return date_time.strftime(Constants.PP_DATETIME_FORMAT)


class RegressionTester:
    DB_NAME = 'regression_testing'

    def __init__(self):
        self._valuation_time_series = []

        # Create and initiate temporary database (will use market data as normal).
        schema = Constants.configs['tables'][Constants.DB_NAME]
        self.temp_directory = os.path.join(Constants.db_path, generate_unique_id(Constants.run_time))
        os.mkdir(self.temp_directory)
        self.db = initiate_database(self.temp_directory, Constants.APP_NAME, schema, Constants.environment)

        self.strategy = None
        self.exchange_cash = 300_000
        self.run_calendar = [Constants.run_time]
        self.run_days = 0

        # Prepare to load bread crumbs, and override database.
        self.bread_crumb_loader = BreadCrumbsDataLoader()
        self.bread_crumb_loader.db = self.db

    def _force_load_required_data(self):
        # Trick strategy object into loading all required data for the run day.

        # Group by day
        run_calendar_days = {}
        for run_datetime in self.run_calendar:
            run_date = run_datetime.replace(hour=0, minute=0, second=0).strftime(Constants.DATETIME_FORMAT)
            if run_date in run_calendar_days:
                run_calendar_days[run_date].append(run_datetime)
            else:
                run_calendar_days[run_date] = [run_datetime]

        last_run_datetimes = [max(run_calendar_days[day]) for day in run_calendar_days]

        for run_datetime in last_run_datetimes:
            original_run_datetime = self.strategy.run_datetime
            self.strategy.run_datetime = run_datetime
            self.strategy.load_required_data(historical_data=True)
            self.strategy.run_datetime = original_run_datetime

    def _run_strategy(self, run_datetime):
        # Initiate trade executor using a simulated exchange.
        exchange = SimulatedExchangeInterface(self.strategy, run_datetime, self.exchange_cash)
        trade_executor = TradeExecutor(self.strategy, exchange)

        # Generate signals.
        self.strategy.run_datetime = run_datetime
        signals = self.strategy.generate_signals()

        # Maybe strategy executor class?
        # run call would look very similar to this method
        # simulate option
        # Optional logging.
        # need a way of adding job for strat execution script
        # Trade Executor Simulator. Can this be abstracted, that way executor and regrestion are always doing the same thing
        if signals:
            set_bread_crumb(self.strategy, signals, None, self.strategy.portfolio.valuate(), db=self.db)
            proposed_trades = trade_executor.generate_trades_from_signals(signals)
            if proposed_trades:
                executed_order_ids = trade_executor.execute_trades(proposed_trades)
                processed_trades = trade_executor.process_executed_trades(executed_order_ids, suppress_log=True)

                # Update save portfolio to database and create way points.
                trade_executor.update_portfolio_db()
                set_bread_crumb(self.strategy, signals, processed_trades, trade_executor.portfolio.valuate(), db=self.db)
            else:
                set_bread_crumb(self.strategy, signals, None, trade_executor.portfolio.valuate(), db=self.db)
        else:
            set_bread_crumb(self.strategy, None, None, self.strategy.portfolio.valuate(), db=self.db)

        # Add valuation to time series.
        self._valuation_time_series.append((run_datetime, trade_executor.portfolio.valuate()))

    def _final_pnl(self):
        if self._valuation_time_series:
            return round(self._valuation_time_series[-1][1] - self._valuation_time_series[0][1], 2)
        else:
            return 0

    def _process_results(self):
        # Return None if regression hasn't been run.
        if not self.strategy.data_loader.data:
            raise Exception('Cannot call _process_results before regression has been run.')

        # Load data.
        self.bread_crumb_loader.load_bread_crumbs_time_series(self.strategy.name)
        bread_crumbs = self.bread_crumb_loader.data[BreadCrumbsDataLoader.BREAD_CRUMBS_TIME_SERIES][self.strategy.name]

        # Group bread crumbs by time.
        times = {}
        for bread_crumb in bread_crumbs:
            timestamp_datetime = datetime.datetime.strptime(bread_crumb[3], Constants.DATETIME_FORMAT)
            if timestamp_datetime in times:
                times[timestamp_datetime].append((bread_crumb[2], bread_crumb[4]))
            else:
                times[timestamp_datetime] = [(bread_crumb[2], bread_crumb[4])]

        # Calculate signal and trade frequency.
        signal_count = 0
        trade_count = 0
        for run_datetime in self.run_calendar:
            if run_datetime.replace(tzinfo=None) in times:
                run_bread_crumbs = times[run_datetime.replace(tzinfo=None)]
                signal_count += 1 if any([True if b[0] == BreadCrumb.SIGNAL and b[1] != '-' else False
                                          for b in run_bread_crumbs]) else 0
                trade_count += 1 if any([True if b[0] == BreadCrumb.TRADE and b[1] != '-' else False
                                         for b in run_bread_crumbs]) else 0

        signal_ratio = round(signal_count / len(self.run_calendar), 2)
        trade_ratio = round(trade_count / len(self.run_calendar), 2)

        return signal_ratio, trade_ratio, self._final_pnl()

    def initiate_strategy(self, xml_path):
        # Parse strategy from xml file.
        strategy_dict = parse_strategy_from_xml(xml_path)
        strategy_setup_dict = parse_strategy_setup_from_xml(xml_path)

        # On-board strategy, portfolio and assets.
        portfolio_id = add_portfolio(self.db, '_{0}_portfolio'.format(
            strategy_dict['name']),
                                     float(strategy_setup_dict['allocation']),
                                     cash=float(strategy_setup_dict['cash'])
                                     )

        add_strategy(self.db, strategy_dict['name'], portfolio_id)

        # Add any assets.
        for asset in strategy_setup_dict['assets']:
            add_assets(self.db, portfolio_id, asset['symbol'], int(asset['units']))

        # Initiate strategy object.
        self.strategy = Strategy(
            self.db,
            strategy_dict['name'],
            strategy_dict['data_requirements'],
            strategy_dict['function'],
            strategy_dict['parameters'],
            RiskProfile(strategy_dict['risk_profile']),
            execution_options=strategy_dict['execution_options']
        )

    def generate_run_calendar(self, start_datetime, end_datetime, run_times):
        # Make list of all days in between start and end date times.
        time_zone = pytz.timezone(Constants.TIME_ZONE)
        no_of_days = (end_datetime - start_datetime).days + 1
        run_days = [(start_datetime + datetime.timedelta(days=i)).astimezone(time_zone) for i in range(no_of_days)]

        # Remove weekends.
        run_days = [d for d in run_days if d.weekday() in [0, 1, 2, 3, 4]]
        self.run_days = len(run_days)

        # Add run times.
        run_calendar = []
        for d in run_days:
            for t in run_times:
                run_calendar.append(d.replace(hour=int(t[:2]), minute=int(t[2:])))

        self.run_calendar = run_calendar

    def run(self, verbose=False):
        # Load all required data.
        Constants.log.info('Force loading required data.')
        self._force_load_required_data()

        # Run regression test.
        Constants.log.info('Running strategy "{0}" for {1} days'.format(self.strategy.name, self.run_days))
        for run_datetime in self.run_calendar:
            Constants.log.info('Running for: {}'.format(run_datetime.strftime(Constants.PP_DATETIME_FORMAT)))
            try:
                # Suppress log.
                if not verbose:
                    previous_log_level = Constants.log.level
                    Constants.log.setLevel(50)

                self._run_strategy(run_datetime)

                # Restore log.
                if verbose:
                    log_hr()
                else:
                    Constants.log.setLevel(previous_log_level)
            except Exception as e:
                run_datetime_string = run_datetime.strftime(Constants.PP_DATETIME_FORMAT)
                Constants.log.warning('Failed to run for {}: {}'.format(run_datetime_string, e))

    def clean_up(self):
        if os.path.isdir(self.temp_directory):
            shutil.rmtree(self.temp_directory)

    def pp_summary_to_log(self):
        log_hr()

        # Print data warnings.
        data_warnings = self.strategy.data_loader.warnings
        if data_warnings:
            Constants.log.info('Data warnings ({})'.format(len(data_warnings)))

        Constants.log.info('REGRESSION SUMMARY')
        log_hr(width=25)

        signal_ratio, trade_ratio, final_pnl = self._process_results()

        Constants.log.info('signals per run: {}'.format(signal_ratio))
        Constants.log.info('trades per run: {}'.format(trade_ratio))

        # Print final profit and loss.
        Constants.log.info('final P&L: {}'.format(final_pnl))

    def export_bread_crumbs_to_csv(self):
        # TODO Separate this out into different files bread crumbs, data warnings, strategy warnings etc.

        # These would normally be written to the db for execution runs, but its is nice to have all data from a
        # regression in an easy-to-access medium, such as a csv file.

        columns_to_print = [3, 2, 4]

        # Load data.
        self.bread_crumb_loader.load_bread_crumbs_time_series(self.strategy.name)
        bread_crumbs = self.bread_crumb_loader.data[BreadCrumbsDataLoader.BREAD_CRUMBS_TIME_SERIES][self.strategy.name]

        # Generate headers.
        headers = Constants.configs['tables'][Constants.APP_NAME][BreadCrumb.TABLE]

        # Generate file path.
        start_date_string = self.run_calendar[0].strftime(Constants.DATETIME_FORMAT[:6])
        end_date_string = self.run_calendar[-1].strftime(Constants.DATETIME_FORMAT[:6])
        file_name = '{}_{}-{}_bread_crumbs.csv'.format(self.strategy.name, start_date_string, end_date_string)
        file_path = os.path.join(Constants.regression_path, file_name)

        signal_ratio, trade_ratio, final_pnl = self._process_results()

        # Generate meta data.
        meta_data = [
            ['regression run at', 'strategy', 'run days', 'signal ratio', 'trade ratio', 'pnl'],
            [Constants.run_time.strftime(Constants.PP_DATETIME_FORMAT),
             self.strategy.name,
             str(self.run_days),
             str(signal_ratio),
             str(trade_ratio),
             str(final_pnl)]
        ]

        # Write bread crumbs to csv file.
        with open(file_path, 'w') as csv_file:
            writer = csv.writer(csv_file)

            # Write meta data.
            writer.writerow(meta_data[0])
            writer.writerow(meta_data[1])

            # Write headers.
            writer.writerow([headers[c] for c in columns_to_print])

            # Write data.
            for row in bread_crumbs:
                row[3] = format_datetime_str(row[3])
                writer.writerow([row[c] for c in columns_to_print])

        Constants.log.info('Bread crumbs exported to {}'.format(file_path))

    def passed_criteria(self):
        if self._final_pnl() > 0:
            return True
        return False


def main():
    # Setup parse options, imitate global constants and logs.
    args = ['start_date', 'end_date', 'run_times', 'verbose', 'out_dir', 'export']
    Constants.parse_arguments(Constants.APP_NAME, custom_args=args)

    # Initiate Job
    job = Job(log_path=Constants.log_path)
    job.log()

    # Initiate. regression test.
    job.update_phase('preparing test')
    regression_tester = RegressionTester()
    regression_tester.initiate_strategy(Constants.xml.path)

    # Generate custom run calendar, default is to run for now.
    if 'start_date' in Constants.configs and 'end_date' in Constants.configs and 'run_times' in Constants.configs:
        start_date = datetime.datetime.strptime(Constants.configs['start_date'], Constants.DATETIME_FORMAT[:6])
        end_date = datetime.datetime.strptime(Constants.configs['end_date'], Constants.DATETIME_FORMAT[:6])
        run_times = Constants.configs['run_times'].split(',')
        regression_tester.generate_run_calendar(start_date, end_date, run_times)
    else:
        raise Exception('Regression tester requires --start_date, --end_date and --run_times are provided.')

    # Run full regression.
    job.update_phase('running regression')
    regression_tester.run(verbose=Constants.configs['verbose'])

    # Print results.
    job.update_phase('processing results')
    Constants.log.info("Testing complete.")
    regression_tester.pp_summary_to_log()

    # Export way point data to CSV report.
    if Constants.configs['export']:
        log_hr()
        regression_tester.export_bread_crumbs_to_csv()

    # Ok, we're finished so lets clean up.
    regression_tester.clean_up()

    # If we got here the regression script ran fine.
    job.finished()

    # Return 0 if strategy passed, return 1 failed.
    if regression_tester.passed_criteria():
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
