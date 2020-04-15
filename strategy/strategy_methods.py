import strategy.strategy_utils as utils


def basic(context, symbol):
    from statistics import mean
    five_minutes_ago = utils.time_minutes_ago(context, 500)

    # Fetch static all data together.
    previous_values = utils.get_values_in_datetime_range(context, symbol, five_minutes_ago, context.now)[:-1]
    latest_value = utils.get_latest_value(context, symbol)

    # Calculate values.
    mean_value = mean(previous_values)
    threshold = mean_value * 0.1

    # Generate signal.
    if latest_value > mean_value + threshold:
        context.signal.sell(symbol, latest_value)
    elif latest_value < mean_value - threshold:
        context.signal.buy(symbol, latest_value)
    else:
        context.signal.hold(symbol)

    # return context.signal
    current_value = utils.get_current_value(context, symbol)
    context.signal.buy(symbol, current_value)
    return context.signal

def pairs(context, symbol):
    from statistics import mean
    current_value = utils.get_current_value(context, symbol)
    # context.signal.buy(symbol, current_value)
    context.signal.hold(symbol)
    return context.signal
