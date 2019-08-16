import os
import subprocess


def get_abs_path(source_file, relative_filename):
    return os.path.abspath(
        os.path.join(
            os.path.dirname(source_file),
            relative_filename
        )
    )


def recursive_listdir(directory):
    return [os.path.join(dp, f) for dp, dn, fn in os.walk(directory) for f in fn]


def clean_up_output_local(output_path):
    subprocess.check_call(['rm', '-rf', output_path])


def list_dir_local(output_path):
    return os.listdir(output_path)


def rename_file_local(old, new):
    os.rename(old, new)


def clean_up_output_hdfs(output_path):
    subprocess.check_call(['hadoop', 'fs', '-rm', '-f', '-R', output_path])


def list_dir_hdfs(path):
    return [
        f.split(' ')[-1].strip().split('/')[-1]
        for f in subprocess.check_output(['hdfs', 'dfs', '-ls', path]).decode().split('\n')
        if f.split(' ')[-1].startswith('hdfs')
    ]


def rename_file_hdfs(old, new):
    subprocess.check_call(['hdfs', 'dfs', '-mv', old, new])


class FileSystemType:
    LOCAL = 1
    HDFS = 2


def util_functions_factory(type=FileSystemType.LOCAL):

    util_funcs = {
        FileSystemType.LOCAL: (clean_up_output_local, list_dir_local, rename_file_local),
        FileSystemType.HDFS: (clean_up_output_hdfs, list_dir_hdfs, rename_file_hdfs)
    }

    return util_funcs[type]
