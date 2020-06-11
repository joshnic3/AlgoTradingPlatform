import csv
import datetime
from collections import namedtuple

from library.bootstrap import Constants
from library.data_loader import BreadCrumbsDataLoader
from library.interfaces.sql_database import Database
from library.strategy.bread_crumbs import BreadCrumbs


def format_datetime_str(datetime_string):
    if datetime_string is None:
        return None
    date_time = datetime.datetime.strptime(datetime_string, Constants.DATETIME_FORMAT)
    return date_time.strftime(Constants.PP_DATETIME_FORMAT)


def evaluate_strategy_bread_crumbs(bread_crumbs):
    timestamp_index = 3
    type_index = 2
    data_index = 4
    crumb_types_counts = {
        BreadCrumbs.TYPES[BreadCrumbs.SIGNALS]: 0,
        BreadCrumbs.TYPES[BreadCrumbs.TRADES]: 0,
        BreadCrumbs.TYPES[BreadCrumbs.DATA_WARNING]: 0,
        BreadCrumbs.TYPES[BreadCrumbs.STRATEGY_ERROR]: 0
    }

    # Calculate number of days the strategy has run for.
    run_days = set(
        [datetime.datetime.strptime(t[timestamp_index], Constants.DATETIME_FORMAT).replace(hour=0, minute=0, second=0)
         for t in bread_crumbs]
    )

    # Count type occurrences, and extract valuations.
    valuations = []
    for bread_crumb in bread_crumbs:
        for crumb_type in crumb_types_counts:
            if bread_crumb[type_index] == crumb_type:
                crumb_types_counts[crumb_type] += 1

            # Extract valuation.
            if bread_crumb[type_index] == BreadCrumbs.TYPES[BreadCrumbs.VALUATION]:
                valuations.append(float(bread_crumb[data_index]))

    # Return result as named tuple.
    Point = namedtuple('Point', 'signal_ratio trade_ratio, pnl, data_warning_count, strategy_error_count, run_days')
    return Point(
        round(crumb_types_counts[BreadCrumbs.TYPES[BreadCrumbs.SIGNALS]] / len(run_days), 2),
        round(crumb_types_counts[BreadCrumbs.TYPES[BreadCrumbs.TRADES]] / len(run_days), 2),
        valuations[-1] - valuations[0] if run_days else 0.0,
        crumb_types_counts[BreadCrumbs.TYPES[BreadCrumbs.DATA_WARNING]],
        crumb_types_counts[BreadCrumbs.TYPES[BreadCrumbs.STRATEGY_ERROR]],
        len(run_days)
    )


def export_strategy_bread_crumbs_to_csv(strategy, csv_file_path, regression_db=None):
    columns_to_print = [3, 2, 4]

    # Load data.
    bread_crumb_loader = BreadCrumbsDataLoader()
    bread_crumb_loader.db = regression_db if regression_db else Database()
    bread_crumb_loader.load_bread_crumbs_time_series(strategy.name)
    bread_crumbs = bread_crumb_loader.data[BreadCrumbsDataLoader.BREAD_CRUMBS_TIME_SERIES][strategy.name]

    # Generate meta data.
    results = evaluate_strategy_bread_crumbs(bread_crumbs)
    meta_data = [
        ['strategy', 'run days', 'signal ratio', 'trade ratio', 'pnl', 'data_warning_count', 'strategy_error_count'],
        [strategy.name, str(results.run_days), str(results.signal_ratio), str(results.trade_ratio), str(results.pnl),
         str(results.data_warning_count), str(results.strategy_error_count)]]

    # Generate bread crumb headers.
    headers = Constants.configs['tables'][Constants.APP_NAME][BreadCrumbs.TABLE]

    # Write bread crumbs to csv file.
    with open(csv_file_path, 'w') as csv_file:
        # Initiate writer.
        writer = csv.writer(csv_file)

        # Write meta data.
        writer.writerow(meta_data[0])
        writer.writerow(meta_data[1])

        # Write bread crumb headers.
        writer.writerow([headers[c] for c in columns_to_print])

        # Write bread crumbs.
        for row in bread_crumbs:
            row[3] = format_datetime_str(row[3])
            writer.writerow([row[c] for c in columns_to_print])

    Constants.log.info('Bread crumbs exported to {}'.format(csv_file_path))
