from library.db_interface import Database


def get_data_source_configs(name, configs):
    db = Database(configs['db_root_path'], 'data_sources', True, configs['environment'])
    condition = 'name="{0}"'.format(name)
    values = ['name', 'configs']
    results = db.query_table('data_sources', condition, values)
    return dict(zip(values, results[0]))

