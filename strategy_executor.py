import sys

from library.bootstrap import Constants
from library.interfaces.sql_database import Database
from library.strategy.strategy import parse_strategy_from_xml
from library.utilities.job import Job
from library.strategy.executor import StrategyExecutor


SUPPRESS_TRADES = 'suppress_trades'
EXPORT_CSV = 'export'


def main():
    # Setup parse options, imitate global constants and logs.
    args = [SUPPRESS_TRADES, EXPORT_CSV]
    Constants.parse_arguments(Constants.APP_NAME, custom_args=args)

    # Setup database.
    db = Database()
    db.log()

    # Initiate Job
    job = Job(log_path=Constants.log_path)
    job.log()

    # Parse strategy xml.
    strategy = parse_strategy_from_xml(Constants.xml.path, return_object=True, db=db)
    Constants.log.info("Strategy portfolio: {0}".format(strategy.portfolio.id))
    db.update_value('strategies', 'updated_by', job.id, 'name="{}"'.format(strategy.name.lower()))

    # Initiate strategy executor
    strategy_executor = StrategyExecutor(strategy, job_object=job, suppress_trades=Constants.configs[SUPPRESS_TRADES])

    # Run strategy.
    strategy_executor.run()

    # Generate report.
    if Constants.configs[EXPORT_CSV]:
        strategy_executor.generate_strategy_report()

    # Wrap it up.
    job.finished()
    return Job.SUCCESSFUL


if __name__ == "__main__":
    sys.exit(main())

