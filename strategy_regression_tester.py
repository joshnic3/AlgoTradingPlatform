import datetime
import os
import shutil
import sys

import pytz

from library.bootstrap import Constants, log_hr
from library.data_loader import BreadCrumbsDataLoader, MarketDataLoader
from library.interfaces.sql_database import initiate_database, generate_unique_id
from library.strategy.bread_crumbs import evaluate_strategy_bread_crumbs
from library.strategy.executor import StrategyExecutor
from library.strategy.reporting import export_strategy_bread_crumbs_to_csv
from library.strategy.risk_profile import RiskProfile
from library.strategy.strategy import parse_strategy_from_xml, parse_strategy_setup_from_xml, Strategy
from library.utilities.job import Job
from library.utilities.onboarding import add_portfolio, add_strategy, add_assets

START_DATE = 'start_date'
END_DATE = 'end_date'
TIMES = 'times'
VERBOSE = 'verbose'
EXPORT = 'export'


def _is_datetime_is_in_the_future(datetime_obj):
    if datetime_obj > Constants.run_time.replace(tzinfo=None):
        return True
    return False


def _is_datetime_later(later_datetime_obj, earlier_datetime_obj):
    if later_datetime_obj > earlier_datetime_obj:
        return True
    return False


class RegressionTester:
    DB_NAME = 'regression_testing'

    def __init__(self, verbose=False):
        self._valuation_time_series = []
        self._verbose = verbose

        # Create temporary directory.
        self.temp_directory = os.path.join(Constants.db_path, generate_unique_id(Constants.run_time))
        os.mkdir(self.temp_directory)

        # Create and initiate temporary ATP database. (Could have the option to use current ATP db)
        schema = Constants.configs['tables'][Constants.DB_NAME]
        self.db = initiate_database(self.temp_directory, Constants.APP_NAME, schema, Constants.environment)

        # Create and initiate temporary market data database. (Probably should copy current market data db)
        schema = Constants.configs['tables'][MarketDataLoader.DB_NAME]
        self.market_data_db = initiate_database(self.temp_directory, MarketDataLoader.DB_NAME, schema,
                                                Constants.environment)

        self.strategy = None
        self.executor = None
        self.exchange_cash = 300000
        self.run_calendar = [Constants.run_time]
        self.run_days = 0

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

        # These should all be the same so max is a the easiest way.
        runs_per_day = max([len(day) for day in run_calendar_days])
        for run_datetime in last_run_datetimes:
            original_run_datetime = self.strategy.run_datetime
            self.strategy.run_datetime = run_datetime
            self.strategy.load_required_data(historical_data=True, required_multiplier=runs_per_day)
            self.strategy.run_datetime = original_run_datetime

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

        # Override market data loader database.
        self.strategy.data_loader.db = self.market_data_db

        # Initiate strategy executor.
        self.executor = StrategyExecutor(self.strategy, suppress_log=not self._verbose, simulate_exchange=True)

    def generate_run_calendar(self, start_datetime, end_datetime, run_times):
        # Make list of all days in between start and end date times.
        time_zone = pytz.timezone(Constants.TIME_ZONE)
        no_of_days = (end_datetime - start_datetime).days + 1
        run_days = [(start_datetime + datetime.timedelta(days=i)).replace(tzinfo=time_zone) for i in range(no_of_days)]

        # Remove weekends.
        run_days = [d for d in run_days if d.weekday() in [0, 1, 2, 3, 4]]
        self.run_days = len(run_days)

        # Add run times.
        run_calendar = []
        for d in run_days:
            for t in run_times:
                run_calendar.append(d.replace(hour=int(t[:2]), minute=int(t[2:])))

        self.run_calendar = run_calendar

    def run(self):
        # Load all required data.
        Constants.log.info('Force loading required data.')
        self._force_load_required_data()

        # Run regression test.
        Constants.log.info('Running strategy "{0}" for {1} days'.format(self.strategy.name, self.run_days))
        for run_datetime in self.run_calendar:
            Constants.log.info('Running for: {}'.format(run_datetime.strftime(Constants.PP_DATETIME_FORMAT)))
            try:
                # Suppress log.
                if not self._verbose:
                    previous_log_level = Constants.log.level
                    Constants.log.setLevel(50)

                # Execute strategy. A new executor is required for each run.
                valuation = self.executor.run(run_datetime_override=run_datetime)

                # Add valuation to time series.
                self._valuation_time_series.append((run_datetime, valuation))

                # Restore log.
                if self._verbose:
                    log_hr()
                else:
                    Constants.log.setLevel(previous_log_level)
            except Exception as e:
                run_datetime_string = run_datetime.strftime(Constants.PP_DATETIME_FORMAT)
                Constants.log.warning('Failed to run for {}: {}'.format(run_datetime_string, e))

    def clean_up(self):
        if os.path.isdir(self.temp_directory):
            shutil.rmtree(self.temp_directory)

    def process_results(self):
        # Load data.
        bread_crumb_loader = BreadCrumbsDataLoader()
        bread_crumb_loader.db = self.db
        bread_crumb_loader.load_bread_crumbs_time_series(self.strategy.name)
        bread_crumbs = bread_crumb_loader.data[BreadCrumbsDataLoader.BREAD_CRUMBS_TIME_SERIES][self.strategy.name]

        # Evaluate results.
        results = evaluate_strategy_bread_crumbs(bread_crumbs)

        # Log summary header.
        log_hr()
        Constants.log.info('REGRESSION SUMMARY')
        log_hr(width=25)

        # Log results.
        Constants.log.info('total runs: {}'.format(results.runs))
        Constants.log.info('data warnings: {}'.format(results.data_warning_count))
        Constants.log.info('strategy errors: {}'.format(results.strategy_error_count))
        Constants.log.info('signals per run: {}'.format(results.signal_ratio))
        Constants.log.info('trades per run: {}'.format(results.trade_ratio))
        Constants.log.info('final P&L: {}'.format(results.pnl))

        # Return True if regression has matched criteria. This can be improved in the future.
        if not results.pnl > 0:
            return False
        if results.signal_ratio < 1.0:
            return False
        if results.strategy_error_count > 1:
            return False
        return True

    def generate_regression_report(self):
        # Regression reports are in their own function as its name requires data from the regression tester object.
        # It also has to override the database the report function uses.

        sub_directory = 'regressions'

        # Generate file name.
        start_date_string = self.run_calendar[0].strftime(Constants.DATETIME_FORMAT[:6])
        end_date_string = self.run_calendar[-1].strftime(Constants.DATETIME_FORMAT[:6])
        file_name = '{}_{}-{}_regression_report.csv'.format(self.strategy.name, start_date_string, end_date_string)

        # Ensure regression sub directory exists.
        repression_reports_directory = os.path.join(Constants.reports_path, sub_directory)
        if not os.path.isdir(repression_reports_directory):
            os.mkdir(repression_reports_directory)

        # Generate csv report using regression database.
        export_strategy_bread_crumbs_to_csv(self.strategy, os.path.join(repression_reports_directory, file_name),
                                            regression_db=self.db)


def main():
    # Setup parse options, imitate global constants and logs.
    args = [START_DATE, END_DATE, TIMES, VERBOSE, EXPORT]
    Constants.parse_arguments(Constants.APP_NAME, custom_args=args)

    # Initiate Job
    job = Job(log_path=Constants.log_path)
    job.log()

    # Initiate. regression test.
    job.update_phase('preparing test')
    regression_tester = RegressionTester(verbose=Constants.configs[VERBOSE])
    regression_tester.initiate_strategy(Constants.xml.path)

    # Generate custom run calendar, default is to run for now.
    if START_DATE in Constants.configs and END_DATE in Constants.configs and TIMES in Constants.configs:
        # Extract dates from command line args.
        start_date = datetime.datetime.strptime(Constants.configs[START_DATE], Constants.DATETIME_FORMAT[:6])
        end_date = datetime.datetime.strptime(Constants.configs[END_DATE], Constants.DATETIME_FORMAT[:6])

        # Validate dates.
        if _is_datetime_is_in_the_future(end_date):
            raise Exception('End date cannot be in the future.')
        if not _is_datetime_later(end_date, start_date):
            raise Exception("End date must be after start date.")

        # Generate run calendar.
        run_times = Constants.configs[TIMES].split(',')
        regression_tester.generate_run_calendar(start_date, end_date, run_times)
    else:
        raise Exception('Regression tester requires --{}, --{} and --{} are provided.'.format(
            START_DATE, END_DATE, TIMES
        ))

    # Run full regression.
    job.update_phase('running regression')
    regression_tester.run()

    # Process results.
    job.update_phase('processing results')
    Constants.log.info("Testing complete.")
    passed = regression_tester.process_results()

    # Export way point data to CSV report.
    if Constants.configs[EXPORT]:
        log_hr()
        regression_tester.generate_regression_report()

    # Ok, we're finished so lets clean up.
    regression_tester.clean_up()

    # If we got here the regression script ran fine.
    job.finished()

    # Return 0 if strategy passed, return 1 failed. This will help facilitate a future strategy CI workflow.
    if passed:
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
