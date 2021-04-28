from math import ceil
import os
import ntpath
import subprocess

from spark.common.utility import logger

import spark.helpers.hdfs_tools as hdfs_tools
import spark.helpers.s3_utils as s3_utils
from datetime import date

# optimal parquet file size is 500mb - 1gb
# smaller means faster writing (to a point)
# larger means faster reading (to a point)
PARQUET_FILE_SIZE = 1024 * 1024 * 500

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


def create_parquet_row_count_file(spark, input_path, output_path, file_name, include_header=True):
    local_path = '/tmp/'
    local_output_file = local_path + file_name

    date_today = date.today()
    files = hdfs_tools.list_parquet_files(input_path, pattern="*.parquet")

    with open(local_output_file, 'w') as output_file:
        if include_header:
            output_file.write("filename|rowcount|moddate\n")
        for file in files:
            cnt = str(spark.read.parquet(file).count())
            output_file.write(
                "{file}|{cnt}|{date_today}\n".format(file=ntpath.basename(file), cnt=cnt, date_today=date_today))

    subprocess.check_call(['aws', 's3', 'cp', local_output_file, output_path])

def get_optimal_partition_count(data_size, expected_file_size=PARQUET_FILE_SIZE):
    """
    Calculates the optimal partition count for a given data size and expected file size
    """
    return int(ceil(data_size / expected_file_size)) or 1

def get_optimal_s3_partition_count(s3_path, expected_file_size=PARQUET_FILE_SIZE):
    """
    Calculates the optimal partition count for a given s3 data path and expected file size
    """
    return get_optimal_partition_count(
        s3_utils.get_file_path_size(s3_path),
        expected_file_size=expected_file_size
    )

def get_optimal_hdfs_partition_count(hdfs_path, expected_file_size=PARQUET_FILE_SIZE):
    """
    Calculates the optimal partition count for a given hdfs data path and expected file size
    """
    return get_optimal_partition_count(
        hdfs_tools.get_hdfs_file_path_size(hdfs_path),
        expected_file_size=expected_file_size
    )
