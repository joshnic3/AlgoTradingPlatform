from library.bootstrap import Constants


class BreadCrumbs:
    TABLE = 'strategy_bread_crumbs'
    SEPARATOR = ':'

    # Bread crumb types.
    GENERAL = 0
    SIGNALS = 1
    TRADES = 2
    VALUATION = 3
    DATA_WARNING = 4
    STRATEGY_ERROR = 5
    TYPES = [
        'general', 'signal', 'trade', 'valuation', 'data_warning', 'strategy_error'
    ]

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

