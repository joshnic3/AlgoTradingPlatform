import sys
import os

from library.onboarding_utils import setup_database_environments, add_twap_required_tickers, add_data_source
from library.file_utils import read_json_file


def main():
    # should be able to set everything up with just "db_root_path" and "root_path" (including data_loader_config and crontab jobs)

    # Args
    script_config_file = '/home/robot/drive/configs/data_loader_config.json'
    environment = 'dev'

    # Parse configs.
    configs = read_json_file(script_config_file)

    # should get schema from script config, one call per script to set up. make sure only creates dirs if has too.
    setup_database_environments(configs['db_root_path'])
    required_tickers = [['0', 'NMR', 'FML', '60', '4'],
                        ['1', 'MSFT', 'FML', '60', '4'],
                        ['2', 'MS', 'FML', '15', '3'],
                        ['3', 'JPM', 'FML', '15', '3']]
    add_twap_required_tickers(configs['db_root_path'], environment, required_tickers)
    add_data_source(configs['db_root_path'], environment, 'FML', os.path.join(configs['root_path'], '/fml_data_source_config.json'))

    return 0


if __name__ == "__main__":
    sys.exit(main())
