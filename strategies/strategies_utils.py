import datetime


def get_latest_value(context, symbol):
    db = context.db
    condition = 'symbol="{0}"'.format(symbol)
    values = 'max(start_time), value'
    results = db.query_table('twaps', condition, values)
    return float(results[0][1])


def get_values_in_datetime_range(context, symbol, from_after, until_before):
    db = context.db
    from_after = from_after.strftime('%Y%m%d%H%M%S')
    until_before = until_before.strftime('%Y%m%d%H%M%S')
    condition = 'symbol="{0}" AND start_time>"{1}" AND end_time<"{2}"'.format(symbol, from_after, until_before)
    results = db.query_table('twaps', condition)
    values_by_start_time = {r[1]: float(r[4]) for r in results}
    values = list(values_by_start_time.values())
    return values


def get_current_value(context, symbol):
    ds = context.ds
    result = ds.request_tickers([symbol, 'JPM'])
    return float(result[symbol])


def time_minutes_ago(context, minutes):
    return context.now - datetime.timedelta(minutes=minutes)

