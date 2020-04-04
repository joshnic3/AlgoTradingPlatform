import datetime
import os
import logging
import sys
from logging.handlers import WatchedFileHandler


def get_log_file_path(root_path, script_name):
    today = datetime.datetime.now()
    today_str = today.strftime("%Y%m%d%H%M%S")
    file_name = '{0}_{1}.log'.format(script_name, today_str)
    log_file_path_template = [root_path, 'logs', file_name]
    return os.path.join(*log_file_path_template)


def setup_log(log_path):
    format = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    log = logging.getLogger('data_loader')

    ch = logging.FileHandler(log_path)
    ch.setFormatter(format)
    log.addHandler(ch)

    sh = logging.StreamHandler()
    sh.setFormatter(format)
    log.addHandler(sh)

    logging.basicConfig(level=logging.INFO)
    return log


def log_configs_as_string(configs):
    # Converts dict to string.
    configs_str_template = ['{0}: {1}'.format(str(key), str(configs[key])) for key in configs.keys()]
    return ', '.join(configs_str_template)


def log_seperator(log):
    log.info('-----------------------------------------------------')
