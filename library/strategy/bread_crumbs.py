from library.bootstrap import Constants
from library.interfaces.sql_database import Database, query_result_to_dict


def set_bread_crumb(strategy, signals, trades, valuation):
    BreadCrumb(strategy=strategy, data=float(valuation), bread_crumb_type=BreadCrumb.VALUATION)
    BreadCrumb(strategy=strategy, data=signals, bread_crumb_type=BreadCrumb.SIGNAL)
    BreadCrumb(strategy=strategy, data=trades, bread_crumb_type=BreadCrumb.TRADE)


class BreadCrumb:
    TABLE = 'strategy_bread_crumbs'

    GENERAL = 'general'
    SIGNAL = 'signal'
    TRADE = 'trade'
    VALUATION = 'valuation'

    def __init__(self, bread_crumb_id=None, strategy=None, data=None, bread_crumb_type=None, db=None):
        self._db = db if db else Database()

        if bread_crumb_id:
            # Load in an existing job from database.
            bread_crumb_row = self._db.get_one_row(self.TABLE, 'id="{0}"'.format(bread_crumb_id))
            bread_crumb_table_schema = Constants.configs['tables'][Constants.DB_NAME][self.TABLE]
            bread_crumb_dict = query_result_to_dict([bread_crumb_row], bread_crumb_table_schema)[0]
        else:
            if strategy is None or data is None:
                raise Exception('Way point constructor requires strategy id and data if not reading database.')
            # Create new job and add it to the database.
            bread_crumb_dict = self._create_bread_crumb_dict(strategy, bread_crumb_type, data)
            self._db.insert_row_from_dict(self.TABLE, bread_crumb_dict)

        # Set instance variables.
        self.id = bread_crumb_dict['id']
        self.strategy_id = bread_crumb_dict['strategy']
        self.timestamp = bread_crumb_dict['timestamp']
        self.data = bread_crumb_dict['data']

    @staticmethod
    def _create_bread_crumb_dict(strategy, bread_crumb_type, data):
        bread_crumb_type = bread_crumb_type if bread_crumb_type else BreadCrumb.GENERAL
        # Extract run time from strategy object to allow way points to be used for regression testing.
        run_datetime_string = strategy.run_datetime.strftime(Constants.DATETIME_FORMAT)
        return {
            'id': str(abs(hash(strategy.name + run_datetime_string))),
            'strategy': strategy.name,
            'type': bread_crumb_type,
            'timestamp': run_datetime_string,
            'data': data
        }
