import os
import shutil
import json


def _check_environment_exists(env):
    # Dont know how to handle this correctly yet.
    environments = ["dev", "test_environment"]
    if env not in environments:
        raise Exception('Environment "{}" does not exist!'.format(env.lower()))


def get_environment_specific_path(root, env):
    env = env.lower()
    _check_environment_exists(env)
    return os.path.join(root, env)


def add_dir(path, overwrite=False, backup=False):
    # if backup:
    #     # TODO add option to back up existing copy
    #     if not os.path.isdir(path):
    #         # Copy dir to path+datetime.now()
    #         pass
    #     pass
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


def write_json_file(json_file_path, content):
    if not os.path.exists(json_file_path):
        # Extract file type for exception
        raise Exception('File not found in path: {}'.format(json_file_path))


def parse_configs_file(cmdline_args):
    # Read script configurations into dict.
    configs = read_json_file(cmdline_args['config_file'])

    # Load cmdline args into configurations dict.
    configs = dict(configs)
    configs.update(cmdline_args)

    # Add default root paths
    configs["db_root_path"] = os.path.join(configs["root_path"], 'data')
    configs["configs_root_path"] = os.path.join(configs["root_path"], 'configs')
    configs["logs_root_path"] = os.path.join(configs["root_path"], 'logs')
    return configs


def edit_config_file(config_file, to_edit, new_value):
    configs = parse_configs_file({'config_file': config_file})
    configs[to_edit] = new_value
    write_json_file(config_file, configs)


