import os

from library.db_interface import Database
from library.file_utils import read_json_file, write_json_file, get_environment_specific_path


def parse_configs():
    # Read script configurations into dict.
    script_config_file = '/Users/joshnicholls/PycharmProjects/algo_trading_platform/drive/configs/data_loader_config.json'
    configs = read_json_file(script_config_file)

    # Read parameters into configurations dict.
    configs['dry_run'] = False
    configs['environment'] = 'dev'
    return configs


def main():
    # Read configs
    configs = parse_configs()

    # Setup db connection.
    db = Database(configs['db_root_path'], configs['environment'], configs['schema'])

    # Testing db
    # values = ['0', 'VOD', 'Bloomberg', '15', '5']
    # db.insert_row('data_requirements', values)
    print('{} rows'.format(len(db.query_table('data_requirements'))))

    return 0


if __name__ == "__main__":
    res = main()
