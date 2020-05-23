import datetime
import json
import os
import shutil


def get_environment_specific_path(root_path, env):
    return os.path.join(root_path, env.lower())


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


def copy_file(source, destination_path):
    if not os.path.exists(source):
        raise Exception('Source file does not exist: {0}'.format(source))
    if os.path.isdir(destination_path):
        raise Exception('Destination path does not exist: {0}'.format(destination_path))
    shutil.copyfile(source, destination_path)


def read_json_file(json_file_path):
    if not os.path.exists(json_file_path):
        # Extract file type for exception
        raise Exception('File not found in path: {}'.format(json_file_path))
    with open(json_file_path, mode='r') as json_file:
        return json.load(json_file)


def write_json_file(json_file_path, content, overwrite=False):
    if overwrite:
        if not os.path.exists(json_file_path):
            raise Exception('File not found in path: {}'.format(json_file_path))
    with open(json_file_path, 'w') as json_file:
        json.dump(content, json_file)


def parse_configs_file(cmdline_args):
    # Add default root paths
    cmdline_args["db_root_path"] = os.path.join(cmdline_args["root_path"], cmdline_args['environment'], 'data')
    cmdline_args["configs_root_path"] = os.path.join(cmdline_args["root_path"], cmdline_args['environment'], 'configs')
    cmdline_args["logs_root_path"] = os.path.join(cmdline_args["root_path"], cmdline_args['environment'], 'logs')

    # Read script configurations into dict.
    config_file_name = '{0}_config.json'.format(cmdline_args['app_name'])
    configs = read_json_file(os.path.join(cmdline_args["configs_root_path"], config_file_name))

    # Load cmdline args into configurations dict.
    configs = dict(configs)
    configs.update(cmdline_args)
    return configs


def edit_config_file(config_file, to_edit, new_value):
    configs = parse_configs_file({'config_file': config_file})
    configs[to_edit] = new_value
    write_json_file(config_file, configs, overwrite=True)


def parse_wildcards(template, wildcards):
    for wildcard in wildcards:
        template = template.replace(wildcard, wildcards[wildcard])
    return template

