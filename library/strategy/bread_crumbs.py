import datetime
from collections import namedtuple

from library.bootstrap import Constants

TIMESTAMP = 3
TYPE = 2
DATA = 4


def format_value_str(value_float):
    return '{:,.2f}'.format(value_float)


def group_bread_crumbs_by_run_time(bread_crumbs, replace_blanks=False):
    # Group by run, this is based off run time, which should be exactly the same for each run
    runs = {}
    for bread_crumb in bread_crumbs:
        if bread_crumb[TYPE] in BreadCrumbs.TYPES:
            run_time = datetime.datetime.strptime(bread_crumb[3], Constants.DATETIME_FORMAT)
            run_time_string = run_time.strftime(Constants.DATETIME_FORMAT)
            data = format_value_str(bread_crumb[DATA]) if bread_crumb[DATA] in BreadCrumbs.VALUE_TYPES \
                else bread_crumb[DATA]
            if run_time_string in runs:
                runs[run_time_string][bread_crumb[TYPE]] = data
            else:
                runs[run_time_string] = {bread_crumb[TYPE]: data}

    # Fill in the blanks.
    run_time_series = []
    for run_time in runs:
        run = runs[run_time]
        run_as_time_series = [run_time]
        for bread_crumb_type in BreadCrumbs.TYPES:
            if bread_crumb_type in run:
                run_as_time_series.append(run[bread_crumb_type])
            else:
                if replace_blanks:
                    run_as_time_series.append('-')
        run_time_series.append(run_as_time_series)
    return run_time_series


def evaluate_strategy_bread_crumbs(bread_crumbs):
    crumb_types_counts = {
        BreadCrumbs.SIGNALS: 0,
        BreadCrumbs.TRADES: 0,
        BreadCrumbs.DATA_WARNING: 0,
        BreadCrumbs.STRATEGY_ERROR: 0
    }

    # Count type occurrences, and extract valuations.
    valuations = []
    for bread_crumb in bread_crumbs:
        for crumb_type in crumb_types_counts:
            if bread_crumb[TYPE] == crumb_type:
                crumb_types_counts[crumb_type] += 1

        # Extract valuation.
        if bread_crumb[TYPE] == BreadCrumbs.VALUATION:
            valuations.append(float(bread_crumb[DATA]))

    # Return result as named tuple.
    Point = namedtuple('Point', 'signal_ratio trade_ratio, pnl, data_warning_count, strategy_error_count, runs')
    return Point(
        round(crumb_types_counts[BreadCrumbs.SIGNALS] / len(valuations), 2),
        round(crumb_types_counts[BreadCrumbs.TRADES] / len(valuations), 2),
        valuations[-1] - valuations[0] if valuations else 0.0,
        crumb_types_counts[BreadCrumbs.DATA_WARNING],
        crumb_types_counts[BreadCrumbs.STRATEGY_ERROR],
        len(valuations)
    )


class BreadCrumbs:
    TABLE = 'strategy_bread_crumbs'
    SEPARATOR = ':'

    # Bread crumb types.
    GENERAL = 'general'
    SIGNALS = 'signal'
    TRADES = 'trade'
    VALUATION = 'valuation'
    DATA_WARNING = 'data_warning'
    STRATEGY_ERROR = 'strategy_error'
    TYPES = [GENERAL, SIGNALS, TRADES, VALUATION, DATA_WARNING, STRATEGY_ERROR]
    VALUE_TYPES = [VALUATION]

    def __init__(self, strategy_name, db):
        self._db = db
        self._strategy_name = strategy_name

    def _create_bread_crumb_dict(self, strategy_run_datetime, bread_crumb_type, data):
        run_datetime_string = strategy_run_datetime.strftime(Constants.DATETIME_FORMAT)
        return {
            'id': str(abs(hash(self._strategy_name + run_datetime_string))),
            'strategy': self._strategy_name,
            'type': bread_crumb_type,
            'timestamp': run_datetime_string,
            'data': data
        }

    def drop(self, now_datetime, bread_crumb_type, data):
        # Naively format data.
        if isinstance(data, list):
            data = self.SEPARATOR.join([str(d) for d in data])

        # Create new job and add it to the database.
        bread_crumb_dict = self._create_bread_crumb_dict(now_datetime, bread_crumb_type, data)
        self._db.insert_row_from_dict(self.TABLE, bread_crumb_dict)

