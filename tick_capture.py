import datetime
import sys

from library.bootstrap import Constants
from library.interfaces.market_data import TickerDataSource
from library.interfaces.sql_database import Database
from library.utilities.job import Job
from library.utilities.xml import get_xml_root, get_xml_element_attribute
from library.data_loader import MarketDataLoader
from library.utilities.onboarding import generate_unique_id


def ticker_checks(ticker):
    #  TODO Implement ticker checks.
    if 'stale_tick_limit' in ticker:
        pass
    return []


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
    subscriptions = []
    for tick_requirement in get_xml_root(Constants.xml.path):
        symbol = get_xml_element_attribute(tick_requirement, 'symbol', required=True)
        stale_tick_limit = get_xml_element_attribute(tick_requirement, 'stale_tick_limit')
        if stale_tick_limit:
            subscriptions.append({'symbol': symbol.upper(), 'stale_tick_limit': int(stale_tick_limit)})
        else:
            subscriptions.append({'symbol': symbol.upper()})
    Constants.log.info('Loaded {0} required tickers.'.format(len(subscriptions)))

    # Load data.
    job.update_phase('requesting data')
    ticker_data_source = TickerDataSource()
    warnings = 0
    for ticker in subscriptions:
        data_source_data = ticker_data_source.request_quote(ticker[TickerDataSource.SYMBOL])
        if data_source_data:
            # Add data to ticker dictionary.
            ticker['price'] = data_source_data[TickerDataSource.PRICE]
            ticker['volume'] = data_source_data[TickerDataSource.VOLUME]

            # Carry out checks on ticker.
            ticker_warnings = ticker_checks(ticker)

            # Save tick to database.
            run_time_string = Constants.run_time.strftime(Constants.DATETIME_FORMAT)
            db.insert_row('ticks', [generate_unique_id(ticker['symbol'] + run_time_string),
                                    run_time_string,
                                    ticker['symbol'],
                                    ticker['price'],
                                    ticker['volume']
                                    ]
                          )

            # Log ticks.
            Constants.log.info('symbol: {0}, price: {1}, volume: {2}'.format(ticker['symbol'], ticker['price'], ticker['volume']))
        else:
            ticker_warnings = ['no_data']

        for warning_type in ticker_warnings:
            warning_id = generate_unique_id(ticker['symbol'] + Constants.run_time.strftime(Constants.DATETIME_FORMAT))
            db.insert_row('data_warnings', [warning_id, 'tick', warning_type, ticker['symbol']])
            Constants.log.info('Could not get data for ticker {0}'.format(ticker['symbol']))
            warnings += 1

    if warnings:
        job.finished(status=Job.WARNINGS, condition='data warnings')
        return Job.WARNINGS
    else:
        job.finished()
        return Job.SUCCESSFUL


if __name__ == "__main__":
    sys.exit(main())
