import csv
import datetime

from library.bootstrap import Constants
from library.data_loader import BreadCrumbsDataLoader
from library.interfaces.sql_database import Database
from library.strategy.bread_crumbs import BreadCrumbs, evaluate_strategy_bread_crumbs


def format_datetime_str(datetime_string):
    if datetime_string is None:
        return None
    date_time = datetime.datetime.strptime(datetime_string, Constants.DATETIME_FORMAT)
    return date_time.strftime(Constants.PP_DATETIME_FORMAT)


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
        ['strategy', 'runs', 'signal ratio', 'trade ratio', 'pnl', 'data_warning_count', 'strategy_error_count'],
        [strategy.name, str(results.runs), str(results.signal_ratio), str(results.trade_ratio), str(results.pnl),
         str(results.data_warning_count), str(results.strategy_error_count)]]

    # Generate bread crumb headers.
    headers = Constants.configs['tables'][Constants.APP_NAME][BreadCrumbs.TABLE]

    # Reverse bread crumbs so latest is shown first.
    bread_crumbs.reverse()

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
