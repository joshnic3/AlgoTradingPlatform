import datetime
from statistics import mean

from library.strategies_utils import get_latest_value, get_values_in_datetime_range, Signal


def basic_strategy(db):
    symbol = 'NMR'
    now = datetime.datetime.now()
    five_minutes_ago = now - datetime.timedelta(minutes=5)

    # Fetch data.
    previous_values = get_values_in_datetime_range(db, symbol, five_minutes_ago, now)[:-1]
    latest_value = get_latest_value(db, symbol)

    # Calculate values.
    mean_value = mean(previous_values)
    threshold = mean_value * 0.1

    # Generate signal.
    signal_id = 0
    signal = Signal(signal_id)
    if latest_value > mean_value + threshold:
        signal.sell(symbol, latest_value)
    elif latest_value < mean_value - threshold:
        signal.buy(symbol, latest_value)
    else:
        signal.hold(symbol)

    return signal

