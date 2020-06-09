import pytz

from library.strategy.signal import Signal
from library.bootstrap import Constants


class Context:
    TABLE = 'strategy_variables'

    def __init__(self, db, strategy_name, run_datetime, data, ds=None):
        self.now = run_datetime.replace(tzinfo=pytz.timezone(Constants.TIME_ZONE))
        self.db = db
        self.data = data
        self.ds = ds if ds else None
        self.strategy_name = strategy_name
        self.signals = []

    def _generate_variable_id(self, variable_name):
        # Variables have to be unique with in a a strategy.
        # Different strategies can use the same variable names with out clashes.
        # TODO Use a consistent hash, python hash function not suitable.
        return str(self.strategy_name + variable_name)

    def add_signal(self, symbol, order_type=Signal.HOLD, target_value=None):
        signal = Signal(len(self.signals))
        if order_type.lower() == Signal.HOLD:
            signal.hold(symbol)
        elif order_type.lower() == Signal.BUY and target_value:
            signal.buy(symbol, target_value)
        elif order_type.lower() == Signal.SELL and target_value:
            signal.sell(symbol, target_value)
        else:
            raise Exception('Signal not valid.')
        self.signals.append(signal)

    def set_variable(self, name, new_value):
        variable_id = self._generate_variable_id(name)
        if self.get_variable(name):
            # update value in db.
            self.db.update_value('strategy_variables', 'value', new_value, 'id="{0}"'.format(variable_id))
        else:
            # insert new variable.
            values = [variable_id, new_value]
            self.db.insert_row('strategy_variables', values)
        return new_value

    def get_variable(self, name, default=None):
        variable_id = self._generate_variable_id(name)
        result = self.db.get_one_row(self.TABLE, 'id="{0}"'.format(variable_id))
        if result:
            return result[1]
        else:
            if default is not None:
                return self.set_variable(name, default)
            return None
