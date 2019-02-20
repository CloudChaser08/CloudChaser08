import subprocess
import os
import re
import spark.helpers.constants as constants

from pyspark.sql.functions import when, col, lit

from datetime import datetime, date


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


def unload(
        spark, runner, df, date_column, partition_name_prefix, provider_partition_value,
        provider_partition_name='part_provider', date_partition_value=None, date_partition_name='part_best_date',
        hvm_historical_date=None, staging_subdir='', columns=None, unload_partition_count=20, test_dir=None,
        skip_rename=False, distribution_key='record_id', substr_date_part=True
):
    """
    Unload normalized data into partitions based on a date column
    """
    NULL_PARTITION_NAME = '0_PREDATES_HVM_HISTORY'

    if columns is None:
        columns = df.columns

    if staging_subdir and staging_subdir[-1] != '/':
        staging_subdir = staging_subdir + '/'

    if test_dir:

        if test_dir[-1] != '/':
            test_dir = test_dir + '/'

        staging_dir = test_dir + staging_subdir
        part_files_cmd = [
            'find', staging_dir + provider_partition_name + '=' + provider_partition_value + '/', '-type', 'f'
        ]
    else:
        staging_dir = constants.hdfs_staging_dir + staging_subdir
        part_files_cmd = [
            'hadoop', 'fs', '-ls', '-R', staging_dir + provider_partition_name + '=' + provider_partition_value + '/'
        ]

    if date_partition_value:
        date_partition_val = lit(date_partition_value)
    else:
        when_clause = col(date_column).isNull()
        if hvm_historical_date:
            # When executed in pyspark, date(Y, m, d) is less than datetime(Y, m, d)
            # Make sure that hvm_historical_date is a date type to avoid this
            # problem
            if type(hvm_historical_date) == datetime:
                hvm_historical_date = hvm_historical_date.date()
            when_clause = (when_clause | (col(date_column) < hvm_historical_date))
        date_column_val = col(date_column).substr(0, 7) if substr_date_part else col(date_column)
        date_partition_val = when(when_clause, lit(NULL_PARTITION_NAME)).otherwise(date_column_val)

    # add partition columns to the total column list
    columns.extend([
        lit(provider_partition_value).alias(provider_partition_name),
        date_partition_val.alias(date_partition_name)
    ])

    # repartition and unload
    df.select(*columns).repartition(unload_partition_count, distribution_key).write.parquet(
        staging_dir, partitionBy=[provider_partition_name, date_partition_name], compression='gzip', mode='append'
    )

    if not skip_rename:
        # add a prefix to part file names
        try:
            part_files = subprocess.check_output(part_files_cmd).strip().split("\n")
        except:
            part_files = []
        spark.sparkContext.parallelize(part_files).repartition(1000).foreach(
            mk_move_file(partition_name_prefix, test_dir is not None)
        )


def partition_and_rename(
        spark, runner, data_type, common_model_script, provider, table_name, date_column, file_date,
        partition_value=None, hvm_historical_date=None, test_dir=None, staging_subdir='',
        distribution_key='record_id', provider_partition='part_provider', date_partition='part_best_date', columns=None,
        unload_partition_count=20
):
    """
    Unload normalized data into partitions based on
    a date column (DEPRECATED - use `unload` function instead)
    """
    old_partition_count = spark.conf.get('spark.sql.shuffle.partitions')

    if hvm_historical_date is not None:
        if type(hvm_historical_date) is not datetime:
            raise Exception("hvm_historical_date should be of type datetime.datetime")

        hvm_historical_date_string = hvm_historical_date.strftime('%Y-%m-%d')

    if columns is None:
        columns = ['*']

    if test_dir:
        staging_dir = test_dir + staging_subdir
        part_files_cmd = ['find', staging_dir + provider_partition + '=' + provider + '/', '-type', 'f']
        common_dirpath = '../common/'

    else:
        staging_dir = constants.hdfs_staging_dir + staging_subdir
        part_files_cmd = ['hadoop', 'fs', '-ls', '-R', staging_dir + provider_partition + '=' + provider + '/']
        common_dirpath = '../../../../common/'

    runner.run_spark_script(common_dirpath + common_model_script, [
        ['table_name', 'final_unload', False],
        ['properties', constants.unload_properties_template.format(provider_partition, date_partition, staging_dir), False],
        ['external', '', False],
        ['additional_columns', '', False]
    ])

    if partition_value is None and hvm_historical_date is None:
        runner.run_spark_script(common_dirpath + 'unload_common_model.sql', [
            ['select_statement', "SELECT {}, '{}' as {}, '0_PREDATES_HVM_HISTORY' as {} FROM {} WHERE {} is NULL".format(
                ','.join(columns), provider, provider_partition, date_partition, table_name, date_column
            ), False],
            ['unload_partition_count', str(unload_partition_count), False],
            ['original_partition_count', old_partition_count, False],
            ['distribution_key', distribution_key, False]
        ])
        runner.run_spark_script(common_dirpath + 'unload_common_model.sql', [
            ['select_statement', "SELECT {0}, '{1}' as {4}, regexp_replace({3}, '-..$', '') as {5} FROM {2} WHERE {3} IS NOT NULL".format(
                ','.join(columns), provider, table_name, date_column, provider_partition, date_partition
            ), False],
            ['unload_partition_count', str(unload_partition_count), False],
            ['original_partition_count', old_partition_count, False],
            ['distribution_key', distribution_key, False]
        ])
    elif partition_value is None and hvm_historical_date is not None:
        runner.run_spark_script(common_dirpath + 'unload_common_model.sql', [
            ['select_statement', "SELECT {0}, '{1}' as {4}, regexp_replace({3}, '-..$', '') as {5} FROM {2} WHERE {3} IS NOT NULL AND {3} >= CAST('{6}' AS DATE)".format(
                ','.join(columns), provider, table_name, date_column, provider_partition, date_partition, hvm_historical_date_string
            ), False],
            ['unload_partition_count', str(unload_partition_count), False],
            ['original_partition_count', old_partition_count, False],
            ['distribution_key', distribution_key, False]
        ])
        runner.run_spark_script(common_dirpath + 'unload_common_model.sql', [
            ['select_statement', "SELECT {0}, '{1}' as {4}, '0_PREDATES_HVM_HISTORY' as {5} FROM {2} WHERE {3} IS NULL OR {3} < CAST('{6}' AS DATE)".format(
                ','.join(columns), provider, table_name, date_column, provider_partition, date_partition, hvm_historical_date_string
            ), False],
            ['unload_partition_count', str(unload_partition_count), False],
            ['original_partition_count', old_partition_count, False],
            ['distribution_key', distribution_key, False]
        ])
    else:
        runner.run_spark_script(common_dirpath + 'unload_common_model.sql', [
            ['select_statement', "SELECT {}, '{}' as {}, '{}' as {} FROM {}".format(
                ','.join(columns), provider, provider_partition, partition_value, date_partition, table_name
            ), False],
            ['unload_partition_count', str(unload_partition_count), False],
            ['original_partition_count', old_partition_count, False],
            ['distribution_key', distribution_key, False]
        ])

    # This will throw an exception if the partition path does not exist which
    # happens when a partition has no data in it and was not created
    try:
        part_files = subprocess.check_output(part_files_cmd).strip().split("\n")
    except:
        part_files = []

    spark.sparkContext.parallelize(part_files).repartition(1000).foreach(
        mk_move_file(file_date, test_dir is not None)
    )


def partition_custom(
        spark, runner, provider, table_name, date_column, file_date,
        partition_value=None, test_dir=None, staging_subdir='',
        provider_partition='part_provider', date_partition='part_best_date', columns=None
):
    """
    Unload custom data into partitions based on a date column
    """
    if columns is None:
        columns = ['*']

    if test_dir:
        staging_dir = "{}{}/{}".format(test_dir, provider, staging_subdir)
        part_files_cmd = ['find', staging_dir, '-type', 'f']
        common_dirpath = '../common/'

    else:
        staging_dir = "{}{}/{}".format(constants.hdfs_staging_dir, provider, staging_subdir)
        part_files_cmd = ['hadoop', 'fs', '-ls', '-R', staging_dir]
        common_dirpath = '../../../../common/'

    model_columns = [[f['name'], f['type']] for f in runner.sqlContext.sql('SELECT * FROM {}'.format(table_name)).schema.jsonValue()['fields']]

    runner.run_spark_script(common_dirpath + 'custom_model.sql', [
        ['table_name', 'final_unload', False],
        ['properties', constants.custom_unload_properties_template.format(date_partition, staging_dir), False],
        ['all_columns', model_columns, False]
    ])

    if partition_value is None and hvm_historical_date is None:
        runner.run_spark_script(common_dirpath + 'unload_custom_model.sql', [
            ['select_statement', "SELECT {}, '0_PREDATES_HVM_HISTORY' as {} FROM {} WHERE {} is NULL".format(
                ','.join(columns), date_partition, table_name, date_column
            ), False],
            ['partitions', '20', False]
        ])
        runner.run_spark_script(common_dirpath + 'unload_custom_model.sql', [
            ['select_statement', "SELECT {0}, regexp_replace({2}, '-..$', '') as {3} FROM {1} WHERE {2} IS NOT NULL".format(
                ','.join(columns), table_name, date_column, date_partition
            ), False],
            ['partitions', '20', False]
        ])
    elif partition_value is None and hvm_historical_date is not None:
        runner.run_spark_script(common_dirpath + 'unload_custom_model.sql', [
            ['select_statement', "SELECT {0}, regexp_replace({2}, '-..$', '') as {3} FROM {1} WHERE {2} IS NOT NULL AND {2} >= CAST('{4}' AS DATE)".format(
                ','.join(columns), table_name, date_column, date_partition, hvm_historical_date_string
            ), False],
            ['partitions', '20', False]
        ])
        runner.run_spark_script(common_dirpath + 'unload_custom_model.sql', [
            ['select_statement', "SELECT {0}, '0_PREDATES_HVM_HISTORY' as {3} FROM {1} WHERE {2} IS NULL OR {2} < CAST('{4}' AS DATE)".format(
                ','.join(columns), table_name, date_column, date_partition, hvm_historical_date_string
            ), False],
            ['partitions', '20', False]
        ])
    else:
        runner.run_spark_script(common_dirpath + 'unload_custom_model.sql', [
            ['select_statement', "SELECT {}, '{}' as {} FROM {}".format(
                ','.join(columns), partition_value, date_partition, table_name
            ), False],
            ['partitions', '20', False]
        ])

    part_files = subprocess.check_output(part_files_cmd).strip().split("\n")

    spark.sparkContext.parallelize(part_files).repartition(1000).foreach(
        mk_move_file(file_date, test_dir is not None)
    )


def distcp(dest, src=constants.hdfs_staging_dir):
    subprocess.check_call(['s3-dist-cp', '--s3ServerSideEncryption',
                           '--src', src,
                           '--dest', dest])
    subprocess.check_call([
        'hdfs', 'dfs', '-rm', '-r', src
    ])


def unload_delimited_file(
        spark, runner, output_path, table_name, test=False, num_files=1, delimiter='|',
        output_file_name_prefix='part-', output_file_name=None
    ):
    "Unload a table to a delimited file at the specified location"
    old_partition_count = spark.conf.get('spark.sql.shuffle.partitions')

    if test:
        common_dirpath = '../common/'

        def clean_up_output():
            subprocess.check_call(['rm', '-rf', output_path])

        def list_dir(path):
            return os.listdir(output_path)

        def rename_file(old, new):
            os.rename(old, new)

    else:
        common_dirpath = '../../../../common/'

        def clean_up_output():
            subprocess.check_call(['hadoop', 'fs', '-rm', '-f', '-R', output_path])

        def list_dir(path):
            return [
                f.split(' ')[-1].strip().split('/')[-1]
                for f in subprocess.check_output(['hdfs', 'dfs', '-ls', path]).split('\n')
                if f.split(' ')[-1].startswith('hdfs')
            ]

        def rename_file(old, new):
            subprocess.check_call(['hdfs', 'dfs', '-mv', old, new])

    clean_up_output()

    runner.run_spark_script(common_dirpath + 'unload_common_model_dsv.sql', [
        ['num_files', str(num_files), False],
        ['delimiter', delimiter, False],
        ['location', output_path],
        ['table_name', table_name, False],
        ['original_partition_count', old_partition_count, False]
    ])

    # rename output files to desired name
    # this step removes the spark hash added to the name by default
    for filename in [f for f in list_dir(output_path) if f[0] != '.']:
        if num_files == 1 and output_file_name is not None:
            rename_file(output_path + filename, output_path + output_file_name)
        else:
            new_name = output_file_name_prefix + re.match('''part-([0-9]+)[.-].*''', filename).group(1) + '.gz'
            rename_file(output_path + filename, output_path + new_name)
