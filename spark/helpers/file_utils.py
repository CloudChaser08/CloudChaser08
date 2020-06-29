import os
import ntpath
import subprocess
from spark.common.utility import logger
from spark.helpers.hdfs_tools import list_parquet_files
from datetime import date

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
    try:
        subprocess.check_call(['hadoop', 'fs', '-rm', '-f', '-R', output_path])
    except Exception as e:
        logger.log(
            "Unable to remove directory: {}\nError encountered: {}".format(output_path, str(e))
        )


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


def create_manifest_file(output_path, file_name):
    local_path = '/tmp/'
    output_file_name = local_path + file_name
    with open(output_file_name, 'w') as output_file:
        subprocess.check_call(['aws', 's3', 'ls', output_path], stdout=output_file)
    subprocess.check_call(['aws', 's3', 'cp', output_file_name, output_path])


def create_parquet_row_count_file(spark, input_path, output_path, file_name):
    local_path = '/tmp/'
    local_output_file = local_path + file_name

    date_today = date.today()
    files = list_parquet_files(input_path, pattern="*.parquet")

    with open(local_output_file, 'w') as output_file:
        for file in files:
            cnt = str(spark.read.parquet(file).count())
            output_file.write(
                "{file}|{cnt}|{date_today}".format(file=ntpath.basename(file), cnt=cnt, date_today=date_today))

    subprocess.check_call(['aws', 's3', 'cp', local_output_file, output_path])


def delete_success_file(s3_path):
    subprocess.check_call(['aws', 's3', 'rm', s3_path + '_SUCCESS'])
