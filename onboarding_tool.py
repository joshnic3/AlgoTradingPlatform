import sys
import os

from library.onboarding_utils import setup_database_environments, add_twap_required_tickers, add_data_source
from library.file_utils import read_json_file


def main():
    # should be able to set everything up with just "db_root_path" and "root_path" (including data_loader_config and crontab jobs)

    # Setup for testing data_loader
    environment = 'staging'
    script_config_file = '/home/drive/algo_trading_platform/configs/data_loader_config.json'
    db_root_path = read_json_file(script_config_file)['db_root_path']
    root_path = read_json_file(script_config_file)['root_path']
    setup_database_environments(db_root_path)
    required_tickers = [['0', 'NMR', 'FML', '15', '4'],
                        ['1', 'MSFT', 'FML', '15', '4'],
                        ['2', 'MS', 'FML', '5', '3'],
                        ['3', 'JPM', 'FML', '5', '3']]
    add_twap_required_tickers(environment, required_tickers)
    add_data_source(environment, 'FML', os.path.join(root_path, 'fml_data_source_config.json'))

    return 0


if __name__ == "__main__":
    sys.exit(main())
