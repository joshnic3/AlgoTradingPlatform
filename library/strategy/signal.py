import datetime


class Signal:

    HOLD = 'hold'
    SELL = 'sell'
    BUY = 'buy'

    def __init__(self, signal_id):
        self.id = signal_id
        self.symbol = None
        self.signal = None
        self.target_value = 0
        self.order_type = 'market'
        self.datetime = datetime.datetime.now()

    def __str__(self):
        order_price = round(self.target_value, 2) if self.order_type != 'market' else 'market'
        string_parts = [self.signal, self.symbol] if self.signal == 'hold' else [self.signal, self.symbol, order_price]
        return ':'.join(string_parts)

    def __repr__(self):
        return self.__str__()

    def sell(self, symbol, price):
        self.symbol = symbol
        self.signal = self.SELL
        self.target_value = price

    def buy(self, symbol, price):
        self.symbol = symbol
        self.signal = self.BUY
        self.target_value = price

    def hold(self, symbol):
        self.symbol = symbol
        self.signal = self.HOLD
        self.target_value = None
