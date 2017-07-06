import subprocess
import spark.helpers.constants as constants
import spark.helpers.file_utils as file_utils
import time


def mk_move_file(prefix, test=False):
    if test:
        mv_cmd = ['mv']
    else:
        mv_cmd = ['hadoop', 'fs', '-mv']

    def move_file(part_file):
        if part_file.find("part-") > -1:
            old_pf = part_file.split(' ')[-1].strip()
            new_pf = '/'.join(old_pf.split('/')[:-1] + [prefix + '_' + old_pf.split('/')[-1]])
            success = False
            retries = 5
            while not success and retries > 0:
                retries -= 1
                try:
                    subprocess.check_call(mv_cmd + [old_pf, new_pf])
                    success = True
                except Exception as e:
                    time.sleep(10)
                    if retries == 0:
                        raise e

    return move_file


def partition_and_rename(spark, runner, data_type, common_model_script, provider, table_name, date_column, file_date, partition_value=None, test_dir=None):
    """
    Unload normalized data into partitions based on
    a date column
    """

    if test_dir:
        staging_dir = test_dir
        part_files_cmd = ['find', staging_dir, '-type', 'f']
        common_dirpath = '../common/'

    else:
        staging_dir = constants.hdfs_staging_dir
        part_files_cmd = ['hadoop', 'fs', '-ls', '-R', staging_dir]
        common_dirpath = '../../../../common/'


    runner.run_spark_script(common_dirpath + common_model_script, [
        ['table_name', 'final_unload', False],
        ['properties', constants.unload_properties_template.format(staging_dir), False]
    ])

    if partition_value is None:
        runner.run_spark_script(common_dirpath + 'unload_common_model.sql', [
            ['select_statement', "SELECT *, '{}' as part_provider, 'NULL' as part_best_date FROM {} WHERE {} is NULL".format(provider, table_name, date_column), False],
            ['partitions', '20', False]
        ])
        runner.run_spark_script(common_dirpath + 'unload_common_model.sql', [
            ['select_statement', "SELECT *, '{0}' as part_provider, regexp_replace({2}, '-..$', '') as part_best_date FROM {1} WHERE {2} IS NOT NULL".format(provider, table_name, date_column), False],
            ['partitions', '20', False]
        ])
    else:
        runner.run_spark_script(common_dirpath + 'unload_common_model.sql', [
            ['select_statement', "SELECT *, '{}' as part_provider, '{}' as part_best_date FROM {}".format(provider, partition_value, table_name), False],
            ['partitions', '20', False]
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
