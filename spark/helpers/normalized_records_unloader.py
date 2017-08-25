import subprocess
import spark.helpers.constants as constants
import spark.helpers.file_utils as file_utils
import time

from datetime import datetime


def mk_move_file(prefix, test=False):
    if test:
        mv_cmd = ['mv']
        ls_cmd = ['ls']
    else:
        mv_cmd = ['hadoop', 'fs', '-mv']
        ls_cmd = ['hadoop', 'fs', '-ls']

    def move_file(part_file):
        if part_file.find("/part-") > -1:
            old_pf = part_file.split(' ')[-1].strip()
            new_pf = '/'.join(old_pf.split('/')[:-1] + [prefix + '_' + old_pf.split('/')[-1]])
            try:
                subprocess.check_call(mv_cmd + [old_pf, new_pf])
            except Exception as e:
                # The move command will fail if the final has already
                # been moved. Check here if the destination file exist
                # and ignore the error if it does
                try:
                    subprocess.check_call(ls_cmd + [new_pf])
                except:
                    raise e

    return move_file


def partition_and_rename(
        spark, runner, data_type, common_model_script, provider, table_name, date_column, file_date,
        partition_value=None, hvm_historical_date=None, test_dir=None, staging_subdir='',
        distribution_key='record_id', provider_partition='part_provider', date_partition='part_best_date'
):
    """
    Unload normalized data into partitions based on
    a date column
    """
    if hvm_historical_date is not None:
        if type(hvm_historical_date) is not datetime:
            raise Exception("hvm_historical_date should be of type datetime.datetime")

        hvm_historical_date_string = hvm_historical_date.strftime('%Y-%m-%d')

    if test_dir:
        staging_dir = test_dir + staging_subdir
        part_files_cmd = ['find', staging_dir + 'part_provider=' + provider + '/', '-type', 'f']
        common_dirpath = '../common/'

    else:
        staging_dir = constants.hdfs_staging_dir + staging_subdir
        part_files_cmd = ['hadoop', 'fs', '-ls', '-R', staging_dir + 'part_provider=' + provider + '/']
        common_dirpath = '../../../../common/'


    runner.run_spark_script(common_dirpath + common_model_script, [
        ['table_name', 'final_unload', False],
        ['properties', constants.unload_properties_template.format(staging_dir), False]
    ])

    if partition_value is None and hvm_historical_date is None:
        runner.run_spark_script(common_dirpath + 'unload_common_model.sql', [
            ['select_statement', "SELECT *, '{}' as {}, '0_PREDATES_HVM_HISTORY' as {} FROM {} WHERE {} is NULL".format(
                provider, provider_partition, date_partition, table_name, date_column
            ), False],
            ['partitions', '20', False],
            ['distribution_key', distribution_key, False]
        ])
        runner.run_spark_script(common_dirpath + 'unload_common_model.sql', [
            ['select_statement', "SELECT *, '{0}' as {3}, regexp_replace({2}, '-..$', '') as {4} FROM {1} WHERE {2} IS NOT NULL".format(
                provider, table_name, date_column, provider_partition, date_partition
            ), False],
            ['partitions', '20', False],
            ['distribution_key', distribution_key, False]
        ])
    elif partition_value is None and hvm_historical_date is not None:
        runner.run_spark_script(common_dirpath + 'unload_common_model.sql', [
            ['select_statement', "SELECT *, '{0}' as {3}, regexp_replace({2}, '-..$', '') as {4} FROM {1} WHERE {2} IS NOT NULL AND {2} >= CAST('{5}' AS DATE)".format(
                provider, table_name, date_column, provider_partition, date_partition, hvm_historical_date_string
            ), False],
            ['partitions', '20', False],
            ['distribution_key', distribution_key, False]
        ])
        runner.run_spark_script(common_dirpath + 'unload_common_model.sql', [
            ['select_statement', "SELECT *, '{0}' as {3}, '0_PREDATES_HVM_HISTORY' as {4} FROM {1} WHERE {2} IS NULL OR {2} < CAST('{5}' AS DATE)".format(
                provider, table_name, date_column, provider_partition, date_partition, hvm_historical_date_string
            ), False],
            ['partitions', '20', False],
            ['distribution_key', distribution_key, False]
        ])
    else:
        runner.run_spark_script(common_dirpath + 'unload_common_model.sql', [
            ['select_statement', "SELECT *, '{}' as {}, '{}' as {} FROM {}".format(
                provider, provider_partition, partition_value, date_partition, table_name
            ), False],
            ['partitions', '20', False],
            ['distribution_key', distribution_key, False]
        ])

    part_files = subprocess.check_output(part_files_cmd).strip().split("\n")

    spark.sparkContext.parallelize(part_files).repartition(1000).foreach(
        mk_move_file(file_date, test_dir is not None)
    )


def distcp(dest):
    subprocess.check_call(['s3-dist-cp', '--s3ServerSideEncryption',
                           '--src', constants.hdfs_staging_dir,
                           '--dest', dest])
    subprocess.check_call([
        'hdfs', 'dfs', '-rm', '-r', constants.hdfs_staging_dir
    ])
