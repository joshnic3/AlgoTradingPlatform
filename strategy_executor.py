import optparse
import os
import sys

from library.bootstrap import Constants
from library.interfaces.exchange import AlpacaInterface as Alpaca
from library.interfaces.sql_database import Database
from library.strategy import parse_strategy_from_xml, WayPoint
from library.trade_executor import TradeExecutor
from library.utilities.file import parse_configs_file
from library.utilities.job import Job
from library.utilities.log import get_log_file_path, setup_log, log_configs


def set_way_points(strategy_name, signals, trades, valuation):
    WayPoint(strategy=strategy_name, data=float(valuation), way_point_type=WayPoint.VALUATION)
    WayPoint(strategy=strategy_name, data=signals, way_point_type=WayPoint.SIGNAL)
    WayPoint(strategy=strategy_name, data=trades, way_point_type=WayPoint.TRADE)


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
        set_way_points(strategy.name, '-', '-', strategy.portfolio.valuate())
        job.finished(condition='calculation error', status=Job.FAILED)
        return Job.FAILED

    if not signals:
        # Script cannot go any further from this point, but should not error.
        set_way_points(strategy.name, '-', '-', strategy.portfolio.valuate())
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
        set_way_points(strategy.name, ', '.join([str(s) for s in signals]), '-', strategy.portfolio.valuate())
        job.finished(condition='exchange is closed', status=Job.WARNINGS)
        return Job.WARNINGS

    # Initiate trade executor.
    job.update_phase('Proposing_trades')
    trade_executor = TradeExecutor(db, strategy, exchange)

    # Prepare trades.
    proposed_trades = trade_executor.generate_trades_from_signals(signals)
    if not proposed_trades:
        # Script cannot go any further from this point, but should not error. Should still update portfolio though.
        trade_executor.update_portfolio_db()
        job.finished(condition='no proposed trades')
        set_way_points(strategy.name, ', '.join([str(s) for s in signals]), '-', trade_executor.portfolio.valuate())
        return Job.WARNINGS

    # Execute trades.
    job.update_phase('Executing_trades')
    executed_order_ids = trade_executor.execute_trades(proposed_trades)

    # Process trades.
    job.update_phase('Processing_trades')
    processed_trades = trade_executor.process_executed_trades(executed_order_ids)

    # Update save portfolio to database and create way point.
    trade_executor.update_portfolio_db()
    set_way_points(strategy.name, ', '.join([str(s) for s in signals]), ', '.join(str(processed_trades)),
                   trade_executor.portfolio.valuate())

    # Log summary.
    Constants.log.info('Executed {0}/{1} trades successfully.'.format(len(processed_trades), len(executed_order_ids)))
    job.finished()
    return Job.SUCCESSFUL


if __name__ == "__main__":
    sys.exit(main())

