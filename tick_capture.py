import datetime
import sys

from library.bootstrap import Constants
from library.interfaces.market_data import TickerDataSource
from library.interfaces.sql_database import Database
from library.utilities.job import Job
from library.utilities.xml import get_xml_root, get_xml_element_attribute
from library.data_loader import MarketDataLoader


def main():
    # Setup parse options, imitate global constants and logs.
    Constants.parse_arguments(Constants.APP_NAME)

    # Initiate Job.
    job = Job(log_path=Constants.log_path)
    job.log()

    # Setup connection to market data database, using the data loader's db name constant.
    db = Database(name=MarketDataLoader.DB_NAME)
    db.log()

    # Parse subscriptions file.
    job.update_phase('parsing subscriptions')
    ticks = []
    for tick_requirement in get_xml_root(Constants.xml.path):
        symbol = get_xml_element_attribute(tick_requirement, 'symbol', required=True)
        stale_tick_limit = get_xml_element_attribute(tick_requirement, 'stale_tick_limit')
        if stale_tick_limit:
            ticks.append({'symbol': symbol.upper(), 'stale_tick_limit': int(stale_tick_limit)})
        else:
            ticks.append({'symbol': symbol.upper()})
    Constants.log.info('Loaded {0} required tickers.'.format(len(ticks)))

    # Load data.
    job.update_phase('requesting data')
    ticker_data_source = TickerDataSource()
    warnings = 0
    for tick in ticks:
        data_source_data = ticker_data_source.request_quote(tick['symbol'])
        if data_source_data:
            tick['price'] = data_source_data[TickerDataSource.PRICE]
            tick['volume'] = data_source_data[TickerDataSource.VOLUME]

            #  TODO Test this is working.
            if 'stale_tick_limit' in tick:
                db.insert_row('data_warnings', [0, 'tick', 'stale_ticker', tick['symbol']])

            # Save tick to database.
            now = datetime.datetime.strftime(datetime.datetime.now(), Constants.DATETIME_FORMAT)
            db.insert_row('ticks', [0, now, tick['symbol'], tick['price']])

            # Log ticks.
            Constants.log.info('symbol: {0}, price: {1}, volume: {2}'.format(tick['symbol'], tick['price'], tick['volume']))
        else:
            db.insert_row('data_warnings', [0, 'tick', 'no_data', tick['symbol']])
            Constants.log.info('Could not get data for ticker {0}'.format(tick['symbol']))
            warnings += 1

    if warnings:
        job.finished(status=Job.WARNINGS, condition='data warnings')
        return Job.WARNINGS
    else:
        job.finished()
        return Job.SUCCESSFUL


if __name__ == "__main__":
    sys.exit(main())
