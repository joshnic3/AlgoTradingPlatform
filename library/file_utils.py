import os
import shutil
import json


def _check_environment_exists(env):
    # Dont know how to handle this correctly yet.
    environments = ["dev", "staging"]
    if env not in environments:
        raise Exception('Environment "{}" does not exist!'.format(env.lower()))


def get_environment_specific_path(root, env):
    env = env.lower()
    _check_environment_exists(env)
    return os.path.join(root, env)


def add_dir(path, overwrite=False):
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


def write_json_file(json_file_path):
    if not os.path.exists(json_file_path):
        # Extract file type for exception
        raise Exception('File not found in path: {}'.format(json_file_path))


def parse_configs_file(cmdline_args):
    # Read script configurations into dict.
    configs = read_json_file(cmdline_args['config_file'])

    # Load cmdline args into configurations dict.
    configs = dict(configs)
    configs.update(cmdline_args)
    return configs


