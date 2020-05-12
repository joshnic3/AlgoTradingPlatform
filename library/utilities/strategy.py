import datetime


def get_latest_value(context, symbol):
    results = context.md_db.query_table('twaps', 'symbol="{0}"'.format(symbol), 'max(start_time), value')
    return float(results[0][1])


def get_values_in_datetime_range(context, symbol, from_after, until_before):
    from_after = from_after.strftime('%Y%m%d%H%M%S')
    until_before = until_before.strftime('%Y%m%d%H%M%S')
    condition = 'symbol="{0}" AND start_time>"{1}" AND end_time<"{2}"'.format(symbol, from_after, until_before)
    results = context.md_db.query_table('twaps', condition)
    values_by_start_time = {r[1]: float(r[4]) for r in results}
    values = list(values_by_start_time.values())
    return values


def get_todays_values(context, symbol):
    now = context.now.strftime('%Y%m%d%H%M%S')
    this_morning = '{0}0000'.format(now[8:])
    return get_values_in_datetime_range(context, symbol, this_morning, now)


def get_live_value(context, symbol):
    result = context.ds.request_tickers([symbol, 'JPM'])
    return float(result[symbol])


def time_minutes_ago(context, minutes):
    return context.now - datetime.timedelta(minutes=minutes)

