import strategy.strategy_utils as utils


def basic(context, symbol):
    from statistics import mean

    # Fetch static all data together.
    eight_hours_ago = utils.time_minutes_ago(context, 60*8)
    previous_values = utils.get_values_in_datetime_range(context, symbol, eight_hours_ago, context.now)
    latest_value = utils.get_live_value(context, symbol)

    # Calculate values.
    mean_value = mean(previous_values)
    threshold = mean_value * 0.1

    # Generate signal.
    if latest_value > mean_value + threshold:
        context.add_signal(symbol, order_type='sell', target_value=latest_value)
    elif latest_value < mean_value - threshold:
        context.add_signal(symbol, order_type='buy', target_value=latest_value)
    else:
        context.add_signal(symbol, order_type='hold')

    return context.signals


def pairs(context, symbol_a, symbol_b):
    # Short term pairs.
    from statistics import mean

    # Get all twaps for both symbols from the last hour.
    one_hour_ago = utils.time_minutes_ago(context, 60)
    a_values = utils.get_values_in_datetime_range(context, symbol_a, one_hour_ago, context.now)
    b_values = utils.get_values_in_datetime_range(context, symbol_b, one_hour_ago, context.now)

    # Get strategy variables.
    mean_relative_difference = context.get_variable('mean_relative_difference')
    a_mean_value = float(context.get_variable('a_mean_value', default=0.0))
    b_mean_value = float(context.get_variable('b_mean_value', default=0.0))

    # Calculate relative difference for last hour.
    relative_differences = [abs(a - b) for a, b in zip(a_values, b_values)]
    current_mean_difference = float(mean(relative_differences))

    # Increase if buying/selling too much, or exhausting capital too quickly.
    threshold = 1.05

    # Generate signal.
    if mean_relative_difference and current_mean_difference > float(mean_relative_difference) * threshold:
        # Decide which ticker is differing from the trend.
        # +ve = up, -ve = down
        a_change_direction = mean(a_values) - a_mean_value
        b_change_direction = mean(b_values) - b_mean_value

        changing_ticker = symbol_a if abs(a_change_direction) > abs(b_change_direction) else symbol_b
        if changing_ticker == symbol_a:
            # If a's value is rising buy b, if a's value is dropping sell b.
            order_type = 'buy' if a_change_direction > 0 else 'sell'
            context.add_signal(symbol_b, order_type=order_type, target_value=utils.get_latest_value(context, symbol_b))
        else:
            # If b's value is rising buy a, if b's value is dropping sell a.
            order_type = 'buy' if b_change_direction > 0 else 'sell'
            context.add_signal(symbol_a, order_type=order_type, target_value=utils.get_latest_value(context, symbol_a))
    else:
        # Hold both.
        context.add_signal(symbol_a)
        context.add_signal(symbol_b)

    # Update context variable.
    context.set_variable('mean_relative_difference', current_mean_difference)
    context.set_variable('a_mean_value', mean(a_values))
    context.set_variable('b_mean_value', mean(b_values))
    return context.signals
