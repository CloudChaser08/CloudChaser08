import os
import argparse
import time
import subprocess
import inspect
import copy
from math import ceil
from spark.runner import PACKAGE_PATH
from pyspark.sql.types import StringType
import pyspark.sql.functions as FN
import spark.common.utility.logger as logger
import spark.helpers.hdfs_utils as hdfs_utils

local_output_path = '/staging/tmp/'
PARQUET_FILE_SIZE = 1024 * 1024 * 350


def get_sql_as_table(sql_path=None):
    if sql_path:
        if sql_path[-1] != '/':
            sql_path += '/'
    else:
        sql_path = os.path.dirname(inspect.getframeinfo(inspect.stack()[0][0]).filename) \
                       .replace(PACKAGE_PATH, "") + '/'

    # SQL scripts have a naming convention of <step_number>_<table_name>.sql
    # Where <step_number> defines the order in which they need to run and
    # <table_name> the name of the table that results from this script
    scripts = [f for f in os.listdir(sql_path) if f.endswith('.sql')]

    # Compare the number of unique step numbers to the number of scripts
    if len(set([int(f.split('_')[0]) for f in scripts])) != len(scripts):
        raise Exception("At least two SQL scripts have the same step number")

    try:
        scripts = sorted(scripts, key=lambda f: int(f.split('_')[0]))
    except:
        raise Exception("At least one SQL script did not follow naming convention <step_number>_<table_name>.sql")

    table_name_list = []
    for s in scripts:
        table_name = '_'.join(s.replace('.sql', '').split('_')[1:])
        table_name_list.append((module, table_name))

    return table_name_list


def sql_unload_and_read(runner, spark, module, output_path, sql_path=None, variables=None):
    if variables is None:
        variables = []

    if sql_path:
        if sql_path[-1] != '/':
            sql_path += '/' + module + '/'
    else:
        sql_path = os.path.dirname(inspect.getframeinfo(inspect.stack()[0][0]).filename) \
                       .replace(PACKAGE_PATH, "") + '/' + module + '/'

    # SQL scripts have a naming convention of <step_number>_<table_name>.sql
    # Where <step_number> defines the order in which they need to run and
    # <table_name> the name of the table that results from this script
    scripts = [f for f in os.listdir(sql_path) if f.endswith('.sql')]

    # Compare the number of unique step numbers to the number of scripts
    if len(set([int(f.split('_')[0]) for f in scripts])) != len(scripts):
        raise Exception("At least two SQL scripts have the same step number")

    try:
        scripts = sorted(scripts, key=lambda f: int(f.split('_')[0]))
    except:
        raise Exception("At least one SQL script did not follow naming convention <step_number>_<table_name>.sql")

    table_name_list = []
    for s in scripts:
        table_name = '_'.join(s.replace('.sql', '').split('_')[1:])
        logger.log('            -loading:' + table_name)
        if module in ['publish_qa']:
            tbl_df = runner.run_spark_script(
                s, variables=copy.deepcopy(variables), source_file_path=sql_path, return_output=True)\
                .withColumn('run_date', FN.current_date())

            tbl_df.repartition(10).write.parquet(local_output_path + table_name + '/',
                                                 compression='gzip', mode='overwrite')

            # Delivery requirement: max file size of 250mb
            # Calculate number of partitions required to maintain a max file size of 250mb
            repartition_cnt = \
                int(ceil(hdfs_utils.get_hdfs_file_path_size(local_output_path + table_name + '/') / PARQUET_FILE_SIZE)) or 1
            logger.log('                -repartition into {} partitions'.format(repartition_cnt))

            tbl_df.repartition(repartition_cnt).write.parquet(output_path + table_name + '/',
                                                              compression='gzip', mode='overwrite')
            logger.log('            -done')
        spark.read.parquet(output_path + table_name + '/').createOrReplaceTempView(table_name)
        table_name_list.append(table_name)

    logger.log('        -sql_unload_and_read {}......Done'.format(module))
    return table_name_list


def sql_unload (runner, spark, module, sql_path=None, variables=None):
    if variables is None:
        variables = []

    if sql_path:
        if sql_path[-1] != '/':
            sql_path += '/' + module + '/'
    else:
        sql_path = os.path.dirname(inspect.getframeinfo(inspect.stack()[0][0]).filename) \
                       .replace(PACKAGE_PATH, "") + '/' + module + '/'

    # SQL scripts have a naming convention of <step_number>_<table_name>.sql
    # Where <step_number> defines the order in which they need to run and
    # <table_name> the name of the table that results from this script
    scripts = [f for f in os.listdir(sql_path) if f.endswith('.sql')]

    # Compare the number of unique step numbers to the number of scripts
    if len(set([int(f.split('_')[0]) for f in scripts])) != len(scripts):
        raise Exception("At least two SQL scripts have the same step number")

    try:
        scripts = sorted(scripts, key=lambda f: int(f.split('_')[0]))
    except:
        raise Exception("At least one SQL script did not follow naming convention <step_number>_<table_name>.sql")

    table_name_list = []
    for s in scripts:
        table_name = '_'.join(s.replace('.sql', '').split('_')[1:])
        logger.log(' -loading:' + table_name)
        tbl_df = runner.run_spark_script(
            s, variables=copy.deepcopy(variables), source_file_path=sql_path, return_output=True)
        logger.log('    -done')
        tbl_df.createOrReplaceTempView(table_name)
        table_name_list.append((module, table_name))

    logger.log(' -sql_unload......Done')
    return table_name_list
