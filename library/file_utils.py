import os
import shutil
import json


def get_environment_specific_path(root, env):
    environments = ['dev']
    # if env.lower() in environments:
    #     return os.path.join(root, env)
    # return None
    return os.path.join(root, env)

def add_dir(path, overwrite=False):
    if os.path.isdir(path) and overwrite:
        shutil.rmtree(path)
    os.mkdir(path)
    return path


def read_json_file(json_file_path):
    if not os.path.exists(json_file_path):
        raise Exception('CSV not found in path: {}'.format(json_file_path))
    with open(json_file_path, mode='r') as json_file:
        return json.load(json_file)


def write_json_file(json_file_path):
    if not os.path.exists(json_file_path):
        raise Exception('CSV not found in path: {}'.format(json_file_path))


