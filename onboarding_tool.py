import sys
import os

from library.onboarding_utils import setup_database_environments, add_twap_required_tickers, add_data_source
from library.file_utils import read_json_file


def main():
    # should be able to set everything up with just "db_root_path" and "root_path" (including data_loader_config and crontab jobs)

    # Setup for testing data_loader
    script_config_file = '/home/drive/algo_trading_platform/configs/data_loader_config.json'
    configs = read_json_file(script_config_file)
    db_root_path = configs['db_root_path']
    setup_database_environments(db_root_path)
    required_tickers = [['0', 'NMR', 'FML', '60', '4'],
                        ['1', 'MSFT', 'FML', '60', '4'],
                        ['2', 'MS', 'FML', '15', '3'],
                        ['3', 'JPM', 'FML', '15', '3']]
    add_twap_required_tickers('dev', required_tickers)
    add_data_source('dev', 'FML', os.path.join(configs['db_root_path'], 'configs/fml_data_source_config.json'))

    return 0


if __name__ == "__main__":
    sys.exit(main())
