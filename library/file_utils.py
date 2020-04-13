import os
import shutil
import json
import datetime
import optparse
import sys

def _check_environment_exists(env):
    # Dont know how to handle this correctly yet.
    environments = ["dev", "staging"]
    if env not in environments:
        raise Exception('Environment "{}" does not exist!'.format(env.lower()))


def get_environment_specific_path(root, env):
    env = env.lower()
    _check_environment_exists(env)
    return os.path.join(root, env)


def add_dir(path, overwrite=False, backup=False):
    if overwrite and backup:
        raise Exception('Cannot overwrite and back up directory at the same time.')
    if os.path.isdir(path) and backup:
        now = datetime.datetime.now()
        parent_dir = os.path.dirname(path)
        backup_path = os.path.join(parent_dir, '{0}_{1}'.format(path, now.strftime('%Y%m%d%H%M%S')))
        shutil.copytree(path, backup_path)
        overwrite = True
    if os.path.isdir(path) and overwrite:
        shutil.rmtree(path)
    os.mkdir(path)
    return path


def read_json_file(json_file_path):
    if not os.path.exists(json_file_path):
        # Extract file type for exception
        raise Exception('File not found in path: {}'.format(json_file_path))
    with open(json_file_path, mode='r') as json_file:
        return json.load(json_file)


# TODO Implement, write and write over, edit_config_file needs to write over.
def write_json_file(json_file_path, content, overwrite=False):
    if not os.path.exists(json_file_path):
        # Extract file type for exception
        raise Exception('File not found in path: {}'.format(json_file_path))


def parse_configs_file(cmdline_args):
    if isinstance(cmdline_args, dict):
        # Read script configurations into dict.
        config_file_name = '{0}_config.json'.format(cmdline_args['app_name'])
        configs = read_json_file(os.path.join(cmdline_args["root_path"], 'configs', config_file_name))

        # Load cmdline args into configurations dict.
        configs = dict(configs)
        configs.update(cmdline_args)
    else:
        configs = read_json_file(cmdline_args)

    # Add default root paths
    configs["db_root_path"] = os.path.join(configs["root_path"], 'data')
    configs["configs_root_path"] = os.path.join(configs["root_path"], 'configs')
    configs["logs_root_path"] = os.path.join(configs["root_path"], 'logs')
    return configs


def edit_config_file(config_file, to_edit, new_value):
    configs = parse_configs_file({'config_file': config_file})
    configs[to_edit] = new_value
    write_json_file(config_file, configs, overwrite=True)


def parse_wildcards(template, wildcards):
    for wildcard in wildcards:
        template = template.replace(wildcard, wildcards[wildcard])
    return template
