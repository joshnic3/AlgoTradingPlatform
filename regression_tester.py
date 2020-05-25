import datetime
import os
import sys

from library.bootstrap import Constants
from library.strategy import parse_strategy_from_xml, parse_strategy_setup_from_xml, Strategy
from library.trade_executor import TradeExecutor
from library.utilities.file import parse_configs_file
from library.utilities.log import get_log_file_path, setup_log, log_configs
from library.interfaces.exchange import SimulatedExchangeInterface
from library.interfaces.sql_database import initiate_database, generate_unique_id
from library.utilities.onboarding import add_portfolio, add_strategy, add_assets


class RegressionTester:
    DB_NAME = 'regression_testing'

    def __init__(self):
        self._valuation_time_series = None
        self._db_path = None

        self.strategy = None

    def initiate_strategy(self, xml_path):
        # Create and initiate temporary database (will use market data as normal).
        db_root_path = Constants.configs['db_root_path']
        schema = Constants.configs['tables'][Constants.db_name]
        environment = Constants.configs['environment']
        db_name = generate_unique_id(datetime.datetime.now())
        db = initiate_database(db_root_path, db_name, schema, environment)
        self._db_path = os.path.join(Constants.configs['db_root_path'], '{}.db'.format(db_name))

        # Parse strategy from xml file.
        strategy_dict = parse_strategy_from_xml(xml_path)
        strategy_setup_dict = parse_strategy_setup_from_xml(xml_path)

        # On-board strategy, portfolio and assets.
        portfolio_id = add_portfolio(db, '_{0}_portfolio'.format(strategy_dict['name']), strategy_setup_dict['cash'])
        add_strategy(db, strategy_dict['name'], portfolio_id)

        # Add any assets.
        for asset in strategy_setup_dict['assets']:
            add_assets(db, portfolio_id, asset['symbol'])

        # Initiate strategy object.
        self.strategy = Strategy(
            db,
            strategy_dict['name'],
            strategy_dict['data_requirements'],
            strategy_dict['function'],
            strategy_dict['parameters'],
            strategy_dict['risk_profile'],
            execution_options=strategy_dict['execution_options']
        )

    def run(self, run_datetime):
        # Generate signals.
        self.strategy.run_datetime = run_datetime
        signals = self.strategy.generate_signals()

        # Trade Executor Simulator
        if signals:
            exchange = SimulatedExchangeInterface(self.strategy.portfolio)
            trade_executor = TradeExecutor(self.strategy, exchange)
            proposed_trades = trade_executor.generate_trades_from_signals(signals)
            if proposed_trades:
                executed_order_ids = trade_executor.execute_trades(proposed_trades)
                trade_executor.process_executed_trades(executed_order_ids, suppress_log=True)
                trade_executor.update_portfolio_db()
                self._valuation_time_series.append(run_datetime, trade_executor.portfolio.valuate())

    def clean_up(self):
        if self._db_path:
            os.remove(self._db_path)

    def final_pnl(self):
        if self._valuation_time_series:
            return self._valuation_time_series[-1][1] - self._valuation_time_series[0][1]
        else:
            return 0


def main():
    # Temp
    xml_path = '/Users/joshnicholls/PycharmProjects/algo_trading_platform/drive/setup/strategies/pairs.xml'
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
        "end_date": '20200525',
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

    # Initiate. regression test.
    regression_tester = RegressionTester()
    regression_tester.initiate_strategy(Constants.configs['xml_path'])

    # Run for each run in calendar.
    Constants.log.info('Running strategy "{0}" for {1} days'.format(regression_tester.strategy.name, len(run_days)))
    previous_log_level = Constants.log.level
    Constants.log.setLevel(50)
    for run_datetime in run_calendar:
        regression_tester.run(run_datetime)
    Constants.log.setLevel(previous_log_level)
    Constants.log.info("Testing complete.")
    regression_tester.clean_up()

    # Process results.
    Constants.log.info("Final PnL: {}".format(regression_tester.final_pnl()))

main()