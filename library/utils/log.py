import datetime
import os
import logging
import library.bootstrap as globals


def get_log_file_path(root_path, job_name):
    today = datetime.datetime.now()
    today_str = today.strftime("%Y%m%d%H%M%S")
    file_name = '{0}_{1}.log'.format(job_name, today_str)
    log_file_path_template = [root_path, file_name]
    return os.path.join(*log_file_path_template)


def setup_log(log_path, show_in_console=False):
    # Setup logging to file.
    logging.root.handlers = []
    log_format = '%(asctime)s|%(levelname)s : %(message)s'
    if globals.configs['debug']:
        logging.basicConfig(level='DEBUG', format=log_format, filename=log_path)
    else:
        logging.basicConfig(level='INFO', format=log_format, filename=log_path)
    log = logging.getLogger('')

    # Setup logging to console.
    if show_in_console:
        console = logging.StreamHandler()
        formatter = logging.Formatter(log_format)
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)
    return log


def log_configs(configs, logger=None):
    if logger is None:
        logger = globals.log
    config_strs = ['Config "{0}": {1}'.format(str(k), str(globals.configs[k])) for k in globals.configs.keys() if globals.configs[k]]
    for config_str in config_strs:
        logger.info(config_str)
    log_hr(logger)


def log_hr(logger=None):
    if logger is None:
        logger = globals.log
    logger.info('-------------------------------------------------------------------------------------------------------')

