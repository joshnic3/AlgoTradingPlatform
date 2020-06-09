import sys

from library.bootstrap import Constants
from library.interfaces.exchange import AlpacaInterface as Alpaca
from library.interfaces.sql_database import Database
from library.strategy.strategy import parse_strategy_from_xml
from library.strategy.bread_crumbs import set_bread_crumb
from library.trade_executor import TradeExecutor
from library.utilities.job import Job

MODE = 'mode'
API_ID = 'API_ID'
API_SECRET_KEY = 'API_SECRET_KEY'

SIMULATE_MODE = 0
EXECUTE_MODE = 1
CUSTOM_ARGS = {
    MODE: ['simulate', 'execute']
}


def main():
    # Setup parse options, imitate global constants and logs.
    args = ['simulate', 'execute', 'withhold_trades']
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

    # Evaluate strategy,
    signals = strategy.generate_signals()

    if signals is None:
        # There was a calculation error. This is fatal.
        set_bread_crumb(strategy, '-', '-', strategy.portfolio.valuate())
        job.finished(condition='calculation error', status=Job.FAILED)
        return Job.FAILED

    if not signals:
        # Script cannot go any further from this point, but should not error.
        set_bread_crumb(strategy, '-', '-', strategy.portfolio.valuate())
        job.finished(condition='no signals')
        return Job.WARNINGS

    # Log signals.
    Constants.log.info('Generated {0} valid signal(s): {1}.'.format(len(signals), ', '.join([str(s) for s in signals])))

    # Initiate exchange.
    if Constants.configs[MODE] == CUSTOM_ARGS[MODE][SIMULATE_MODE]:
        exchange = Alpaca(Constants.configs[API_ID], Constants.configs[API_SECRET_KEY], simulator=True)
    elif Constants.configs[MODE] == CUSTOM_ARGS[MODE][EXECUTE_MODE]:
        exchange = Alpaca(Constants.configs[API_ID], Constants.configs[API_SECRET_KEY])
    else:
        # Script cannot go any further from this point.
        raise Exception('Mode "{0}" is not valid.'.format(Constants.configs[MODE]))
    if not exchange.is_exchange_open():
        # Script cannot go any further from this point, but should not error.
        set_bread_crumb(strategy, ', '.join([str(s) for s in signals]), '-', strategy.portfolio.valuate())
        job.finished(condition='exchange is closed', status=Job.WARNINGS)
        return Job.WARNINGS

    # Initiate trade executor.
    job.update_phase('Proposing_trades')
    trade_executor = TradeExecutor(strategy, exchange)

    # Prepare trades.
    proposed_trades = trade_executor.generate_trades_from_signals(signals)
    if not proposed_trades:
        # Script cannot go any further from this point, but should not error. Should still update portfolio though.
        trade_executor.update_portfolio_db()
        job.finished(condition='no proposed trades')
        set_bread_crumb(strategy, ', '.join([str(s) for s in signals]), '-', trade_executor.portfolio.valuate())
        return Job.WARNINGS

    # This is so the web service can set off dry runs (maybe remove dru_run constant as it is not universal).
    if Constants.configs['withhold_trades']:
        Constants.log.info('Produced {0} trade(s).'.format(len(proposed_trades)))
    else:
        # Execute trades.
        job.update_phase('Executing_trades')
        executed_order_ids = trade_executor.execute_trades(proposed_trades)

        # Process trades.
        job.update_phase('Processing_trades')
        processed_trades = trade_executor.process_executed_trades(executed_order_ids)

        # Log summary.
        Constants.log.info('Executed {0}/{1} trades successfully.'.format(len(processed_trades), len(executed_order_ids)))

    # Update save portfolio to database and create way point.
    trade_executor.update_portfolio_db()
    processed_trades_string = ', '.join(processed_trades) if processed_trades else '-'
    signals_string = ', '.join([str(s) for s in signals])
    set_bread_crumb(strategy, signals_string, processed_trades_string, trade_executor.portfolio.valuate())

    job.finished()
    return Job.SUCCESSFUL


if __name__ == "__main__":
    sys.exit(main())

