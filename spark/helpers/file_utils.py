"""file utils"""
from math import ceil
import os
import ntpath
import subprocess

import spark.helpers.hdfs_utils as hdfs_utils
import spark.helpers.s3_utils as s3_utils
from datetime import date
from spark.helpers.constants import PARQUET_FILE_SIZE


def get_abs_path(source_file, relative_filename):
    return os.path.abspath(
        os.path.join(
            os.path.dirname(source_file),
            relative_filename
        )
    )


def recursive_listdir(directory):
    return [os.path.join(dp, f) for dp, dn, fn in os.walk(directory) for f in fn]


def create_manifest_file(output_path, file_name):
    local_path = '/tmp/'
    output_file_name = local_path + file_name
    with open(output_file_name, 'w') as output_file:
        output_file.writelines(s3_utils.list_files(output_path))

    s3_utils.copy_file_from_local(output_file_name, output_path)


def create_parquet_row_count_file(spark, input_path, output_path,
                                  file_name, include_header=True):
    local_path = '/tmp/'
    local_output_file = local_path + file_name

    date_today = date.today()
    files = hdfs_utils.list_parquet_files(input_path, pattern="*.parquet")

    with open(local_output_file, 'w') as output_file:
        if include_header:
            output_file.write("filename|rowcount|moddate\n")
        for file in files:
            cnt = str(spark.read.parquet(file).count())
            output_file.write(
                "{file}|{cnt}|{date_today}\n".format(
                    file=ntpath.basename(file),
                    cnt=cnt,
                    date_today=date_today)
            )

    s3_utils.copy_file_from_local(local_output_file, output_path)


def get_optimal_partition_count(data_size,
                                expected_file_size=PARQUET_FILE_SIZE):
    """
    Calculates the optimal partition count for a given data size and expected file size
    """
    return int(ceil(data_size / expected_file_size)) or 1


def get_optimal_s3_partition_count(s3_path,
                                   expected_file_size=PARQUET_FILE_SIZE):
    """
    Calculates the optimal partition count for a given s3 data path and expected file size
    """
    return get_optimal_partition_count(
        s3_utils.get_file_path_size(s3_path),
        expected_file_size=expected_file_size
    )


def get_optimal_hdfs_partition_count(hdfs_path,
                                     expected_file_size=PARQUET_FILE_SIZE):
    """
    Calculates the optimal partition count for a given hdfs data path and expected file size
    """
    return get_optimal_partition_count(
        hdfs_utils.get_hdfs_file_path_size(hdfs_path),
        expected_file_size=expected_file_size
    )


def clean_up_output_local(output_path):
    subprocess.check_call(['rm', '-rf', output_path])


def list_dir_local(output_path):
    return os.listdir(output_path)


def rename_file_local(old, new):
    os.rename(old, new)


class FileSystemType:
    LOCAL = 1
    HDFS = 2


def util_functions_factory(system_type=FileSystemType.LOCAL):
    util_funcs = {
        FileSystemType.LOCAL: (clean_up_output_local, list_dir_local, rename_file_local),
        FileSystemType.HDFS: hdfs_utils.HDFS_STANDARD_FUNCTIONS
    }

    return util_funcs[system_type]
