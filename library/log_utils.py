import datetime
import os
import logging


def get_log_file_path(root_path, script_name):
    today = datetime.datetime.now()
    today_str = today.strftime("%Y%m%d%H%M%S")
    file_name = '{0}_{1}.log'.format(script_name, today_str)
    log_file_path_template = [root_path, file_name]
    return os.path.join(*log_file_path_template)


def setup_log(log_path, show_in_console=False):
    # Setup logging to file.
    logging.root.handlers = []
    log_format = '%(asctime)s: %(message)s'
    logging.basicConfig(level='INFO', format=log_format, filename=log_path)
    log = logging.getLogger('')

    # Setup logging to console.
    if show_in_console:
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter(log_format)
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)
    return log


def log_configs(configs, log):
    config_strs = ['Config "{0}": {1}'.format(str(key), str(configs[key])) for key in configs.keys()]
    for config_str in config_strs:
        log.info(config_str)


def log_hr(log):
    log.info('-----------------------------------------------------')


def log_end_status(log, script_name, status):
    status_map = {0: "SUCCESSFULLY",
                  1: "with ERRORS",
                  2: "with WARNINGS"}

    if status in status_map:
        log.info('{0} finished {1}!'.format(script_name, status_map[status]))
    else:
        log.info('{0} failed with status {1}!'.format(script_name, status))

