from library.strategy import parse_strategy_from_xml
import datetime
from library.bootstrap import Constants
from library.utilities.log import get_log_file_path, setup_log, log_configs
from library.utilities.file import parse_configs_file
import os
import sys


class RegressionTester:

    def __init__(self):
        self.strategy = None
        self.signals = []
        self.historical_values_time_series = None
        self.final_pnl = 0

    def initiate_strategy(self, xml_path):
        self.strategy = parse_strategy_from_xml(xml_path, return_object=True)

    def run(self, run_datetime):
        # Evaluate signals.
        self.strategy.run_datetime = run_datetime
        signals = self.strategy.generate_signals()
        self.signals.append(signals)

        # Trade Executor Simulator


def main():
    # Temp
    xml_path = '/Users/joshnicholls/PycharmProjects/algo_trading_platform/drive/setup/algo_trading_platform_setup/strategies/pairs.xml'
    root_path = '/Users/joshnicholls/PycharmProjects/algo_trading_platform/drive'

    # Load configs.
    Constants.configs = parse_configs_file({
        "app_name": 'algo_trading_platform',
        "environment": 'dev',
        "root_path": root_path,
        "job_name": 'regression_test',
        "script_name": str(os.path.basename(sys.argv[0])).split('.')[0],
        "dry_run": False,
        "debug": False,

        # Parse script specific args.
        "start_date": '20200501',
        "end_date": '20200518',
        "run_times": '0900,0930,1000,1030',
        "xml_path": xml_path
    })

    # Setup logging.
    log_path = get_log_file_path(Constants.configs['logs_root_path'], 'regression_tester')
    Constants.log = setup_log(log_path, True if Constants.configs['environment'] == 'dev' else False)
    log_configs(Constants.configs)

    # Create run calender. [run_datetime]
    start_date = datetime.datetime.strptime(Constants.configs['start_date'], Constants.date_time_format[:6])
    end_date = datetime.datetime.strptime(Constants.configs['end_date'], Constants.date_time_format[:6])
    run_days = [start_date + datetime.timedelta(days=i) for i in range((end_date - start_date).days + 1)]

    # Remove weekends.
    run_days = [d for d in run_days if d.weekday() in [0, 1, 2, 3, 4, 6]]

    # Add run times.
    run_calendar = []
    for d in run_days:
        for t in Constants.configs['run_times'].split(','):
            run_calendar.append(d.replace(hour=int(t[:2]), minute=int(t[2:])))

    # Run regression test.
    regression_tester = RegressionTester()
    regression_tester.initiate_strategy(Constants.configs['xml_path'])
    Constants.log.info('Running strategy "{0}" for {1} days'.format(regression_tester.strategy.name, len(run_days)))
    previous_log_level = Constants.log.level
    Constants.log.setLevel(50)
    for run_datetime in run_calendar:
        regression_tester.run(run_datetime)
    Constants.log.setLevel(previous_log_level)
    Constants.log.info("Testing complete.")

    # Process results.
    print(regression_tester.signals)
    print(regression_tester.final_pnl)

main()