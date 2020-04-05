import sys
import os

from library.onboarding_utils import setup_database_environments, add_twap_required_tickers, add_data_source
from library.file_utils import read_json_file
from library.log_utils import get_log_file_path, setup_log, log_configs_as_string, log_seperator, log_end_status


def main():
    status = -1
    # Read configs
    cmdline_args = None
    # global configs
    # configs = parse_configs('/Users/joshnicholls/PycharmProjects/algo_trading_platform/drive/configs/data_loader_config.json', cmdline_args)

    # Setup logging.
    script_name = str(os.path.basename(sys.argv[0]))
    log_path = get_log_file_path(configs['root_path'], script_name.split('.')[0])
    log = setup_log(log_path)
    log.info('Configs: {}'.format(log_configs_as_string(configs)))


    # Setup for testing data_loader
    script_config_file = '/Users/joshnicholls/PycharmProjects/algo_trading_platform/drive/configs/data_loader_config.json'
    db_root_path = read_json_file(script_config_file)['db_root_path']
    setup_database_environments(db_root_path)
    required_tickers = [['0', 'NMR', 'FML', '20', '5'],
                        ['1', 'MSFT', 'FML', '20', '5'],
                        ['2', 'MS', 'FML', '10', '2'],
                        ['3', 'JPM', 'FML', '10', '2']]
    add_twap_required_tickers('dev', required_tickers)
    add_data_source('dev', 'FML', '/Users/joshnicholls/PycharmProjects/algo_trading_platform/drive/configs/fml_data_source_config.json')

    status = 0
    log_end_status(log, script_name, status)
    return 0


if __name__ == "__main__":
    sys.exit(main())
