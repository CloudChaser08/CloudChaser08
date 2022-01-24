"""normalized records loader"""
import subprocess
import re
import time
import spark.helpers.constants as constants
import spark.helpers.file_utils as file_utils
from spark.helpers.file_utils import FileSystemType
from spark.helpers.hdfs_utils import get_hdfs_file_count, list_parquet_files, clean_up_output_hdfs
from spark.helpers.s3_utils import get_s3_file_count, list_files
from spark.helpers.manifest_utils import write as write_manifests
from spark.helpers.manifest_utils import list as list_manifest_files
from spark.helpers.manifest_utils import OUTPUT_DIR
from pyspark.sql.functions import when, col, lit

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


def unload(
        spark, runner, df, date_column, partition_name_prefix, provider_partition_value,
        provider_partition_name='part_provider', date_partition_value=None, date_partition_name='part_best_date',
        hvm_historical_date=None, staging_subdir='', columns=None, unload_partition_count=20, test_dir=None,
        skip_rename=False, distribution_key='record_id', substr_date_part=True,
        partition_by_part_file_date=False
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
            'find', staging_dir +
                    provider_partition_name + '=' + provider_partition_value + '/', '-type', 'f'
        ]
    else:
        staging_dir = constants.hdfs_staging_dir + staging_subdir
        part_files_cmd = [
            'hadoop', 'fs', '-ls', '-R',
            staging_dir + provider_partition_name + '=' + provider_partition_value + '/'
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
    partition_by = [provider_partition_name, date_partition_name]
    df = df.select(*columns)

    if partition_by_part_file_date:
        part_file_date = 'part_file_date'
        df = df.withColumn(part_file_date, lit(partition_name_prefix))
        partition_by.insert(1, part_file_date)

    df.repartition(unload_partition_count, distribution_key).write.parquet(
        staging_dir, partitionBy=partition_by, compression='gzip',
        mode='append'
    )

    if not skip_rename:
        # add a prefix to part file names
        try:
            part_files = subprocess.check_output(part_files_cmd).decode().strip().split("\n")
        except:
            part_files = []
        spark.sparkContext.parallelize(part_files).repartition(1000).foreach(
            mk_move_file(partition_name_prefix, test_dir is not None)
        )


def partition_and_rename(
        spark, runner, data_type, common_model_script, provider, table_name, date_column, file_date,
        partition_value=None, hvm_historical_date=None, test_dir=None, staging_subdir='',
        distribution_key='record_id', provider_partition='part_provider',
        date_partition='part_best_date', columns=None, unload_partition_count=20
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
        part_files_cmd = ['find', staging_dir + provider_partition + '=' + provider + '/',
                          '-type', 'f']
        common_dirpath = '../common/'

    else:
        staging_dir = constants.hdfs_staging_dir + staging_subdir
        part_files_cmd = ['hadoop', 'fs', '-ls', '-R',
                          staging_dir + provider_partition + '=' + provider + '/']
        common_dirpath = '../../../../common/'

    runner.run_spark_script(common_dirpath + common_model_script, [
        ['table_name', 'final_unload', False],
        ['properties'
         , constants.unload_properties_template.format(provider_partition, date_partition
                                                       , staging_dir), False],
        ['external', '', False],
        ['additional_columns', '', False]
    ])

    if partition_value is None and hvm_historical_date is None:
        runner.run_spark_script(common_dirpath + 'unload_common_model.sql', [
            ['select_statement',
             "SELECT {}, '{}' as {}, '0_PREDATES_HVM_HISTORY' as {} FROM {} WHERE {} is NULL".format(
                 ','.join(columns), provider, provider_partition, date_partition, table_name, date_column), False],
            ['unload_partition_count', str(unload_partition_count), False],
            ['original_partition_count', old_partition_count, False],
            ['distribution_key', distribution_key, False]
        ])
        runner.run_spark_script(common_dirpath + 'unload_common_model.sql', [
            ['select_statement',
             "SELECT {0}, '{1}' as {4}, regexp_replace({3}, '-..$', '') as {5} FROM {2} WHERE {3} IS NOT NULL".format(
                 ','.join(columns), provider, table_name, date_column, provider_partition, date_partition), False],
            ['unload_partition_count', str(unload_partition_count), False],
            ['original_partition_count', old_partition_count, False],
            ['distribution_key', distribution_key, False]
        ])
    elif partition_value is None and hvm_historical_date is not None:
        runner.run_spark_script(common_dirpath + 'unload_common_model.sql', [
            ['select_statement',
             "SELECT {0}, '{1}' as {4}, regexp_replace({3}, '-..$', '') as {5} FROM {2} "
             "WHERE {3} IS NOT NULL AND {3} >= CAST('{6}' AS DATE)".format(
                 ','.join(columns), provider, table_name, date_column, provider_partition,
                 date_partition, hvm_historical_date_string), False],
            ['unload_partition_count', str(unload_partition_count), False],
            ['original_partition_count', old_partition_count, False],
            ['distribution_key', distribution_key, False]
        ])
        runner.run_spark_script(common_dirpath + 'unload_common_model.sql', [
            ['select_statement',
             "SELECT {0}, '{1}' as {4}, '0_PREDATES_HVM_HISTORY' as {5} FROM {2} "
             "WHERE {3} IS NULL OR {3} < CAST('{6}' AS DATE)".format(
                ','.join(columns), provider, table_name, date_column, provider_partition
                , date_partition, hvm_historical_date_string), False],
            ['unload_partition_count', str(unload_partition_count), False],
            ['original_partition_count', old_partition_count, False],
            ['distribution_key', distribution_key, False]
        ])
    else:
        runner.run_spark_script(common_dirpath + 'unload_common_model.sql', [
            ['select_statement', "SELECT {}, '{}' as {}, '{}' as {} FROM {}".format(
                ','.join(columns), provider, provider_partition, partition_value,
                date_partition, table_name
            ), False],
            ['unload_partition_count', str(unload_partition_count), False],
            ['original_partition_count', old_partition_count, False],
            ['distribution_key', distribution_key, False]
        ])

    # This will throw an exception if the partition path does not exist which
    # happens when a partition has no data in it and was not created
    try:
        part_files = subprocess.check_output(part_files_cmd).decode().strip().split("\n")
    except:
        part_files = []

    spark.sparkContext.parallelize(part_files).repartition(1000).foreach(
        mk_move_file(file_date, test_dir is not None)
    )


def partition_custom(
        spark, runner, provider, table_name, date_column, file_date,
        partition_value, test_dir=None, staging_subdir='',
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

    model_columns = [[f['name'], f['type']] for f in runner.sqlContext.sql(
        'SELECT * FROM {}'.format(table_name)).schema.jsonValue()['fields']]

    runner.run_spark_script(common_dirpath + 'custom_model.sql', [
        ['table_name', 'final_unload', False],
        ['properties', constants.custom_unload_properties_template.format(date_partition,
                                                                          staging_dir), False],
        ['all_columns', model_columns, False]
    ])

    runner.run_spark_script(common_dirpath + 'unload_custom_model.sql', [
        ['select_statement', "SELECT {}, '{}' as {} FROM {}".format(
            ','.join(columns), partition_value, date_partition, table_name
        ), False],
        ['partitions', '20', False]
    ])

    part_files = subprocess.check_output(part_files_cmd).decode().strip().split("\n")

    spark.sparkContext.parallelize(part_files).repartition(1000).foreach(
        mk_move_file(file_date, test_dir is not None)
    )


def distcp(dest,
           src=constants.hdfs_staging_dir,
           server_side_encryption=True,
           file_chunk_size=1000,
           deleteOnSuccess = True):
    """Uses s3-dist-cp to copy files from hdfs to s3.
    In the case that more than `file_chunk_size` files are to be copied, then these files will
    be uploaded in chunks. Where each chunk consists of no more than the value set
    in `file_chunk_size`. Otherwise, all files will be uploaded in a single operation.

    Args:
        dest (str): A URI that points to where the files should be copied to.
        src (str, optional): A URI that points to the source directory that
            contains the files to be copied. Default is, /staging/.
        server_side_encryption (bool, optional): Determines whether or
            not the copied files should be encrypted once they're copied
            to their target destination. Default is, Ture.
        deleteOnSuccess (bool, optional): Determines whether files should be
            deleted once they're copyied. Default is True.

            Note:
                server_side_encryption only works when copying files over to
                s3. In addition, the target bucket must require an encryption.
                If the above two requirements are not met, the command will
                fail.
        file_chunk_size (int, optional): The number of files to upload at once to s3.
            Default is, 5000.
    """

    if src and src[-1] != '/': src = src + '/'
    if dest[-1] != '/': dest = dest + '/'

    dist_cp_command = [
        's3-dist-cp',
        '--s3ServerSideEncryption',
        '--deleteOnSuccess',
        '--src', src,
        '--dest', dest
    ]

    if not deleteOnSuccess:
        dist_cp_command.remove('--deleteOnSuccess')

    if get_hdfs_file_count(src) > file_chunk_size:
        if not server_side_encryption:
            dist_cp_command.remove('--s3ServerSideEncryption')

        files = list_parquet_files(src)
        write_manifests(files)
        file_names = list_manifest_files(OUTPUT_DIR)

        for file_name in file_names:
            subprocess.check_call(dist_cp_command + ['--srcPrefixesFile', file_name])

        clean_up_output_hdfs(''.join(['hdfs://', OUTPUT_DIR]))
    else:
        subprocess.check_call(dist_cp_command)


def timed_distcp(dest,
                 src=constants.hdfs_staging_dir,
                 server_side_encryption=True,
                 file_chunk_size=5000,
                 deleteOnSuccess = True):
    """Uses s3-dist-cp to copy files from hdfs to s3 and returns the commands runtime.
    In the case that more than `file_chunk_size` files are to be copied, then these files will
    be uploaded in chunks. Where each chunk consists of no more than the value set
    in `file_chunk_size`. Otherwise, all files will be uploaded in a single operation.

    Args:
        dest (str): A URI that points to where the files should be copied to.
        src (str, optional): A URI that points to the source directory that
            contains the files to be copied. Default is, /staging/.
        server_side_encryption (bool, optional): Determines whether or
            not the copied files should be encrypted once they're copied
            to their target destination. Default is, Ture.

            Note:
                server_side_encryption only works when copying files over to
                s3. In addition, the target bucket must require an encryption.
                If the above two requirements are not met, the command will
                fail.
        file_chunk_size (int, optional): The number of files to upload at once to s3.
            Default is, 5000.

    Returns:
        run_time (int): The total time it took for s3-dist-cp to run in seconds.
    """

    start = time.time()
    distcp(dest, src, server_side_encryption, file_chunk_size, deleteOnSuccess)

    return time.time() - start


def unload_delimited_file(
        spark, runner, output_path, table_name, test=False, num_files=1, delimiter='|',
        output_file_name_prefix='part-', output_file_name=None, output_file_name_template=None,
        header=False, quote=True, compression='gzip'):
    """Unload a table to a delimited file at the specified location"""
    common_dirpath = "../common/" if test else "../../../../common/"

    file_type = FileSystemType.LOCAL if test else FileSystemType.HDFS

    clean_up_output, list_dir, rename_file = file_utils.util_functions_factory(file_type)

    clean_up_output(output_path)

    tbl = spark.table(table_name)
    tbl.select(*[col(c).cast('string').alias(c) for c in tbl.columns]).createOrReplaceTempView('for_delimited_output')

    (
        spark.table('for_delimited_output')
        .repartition(num_files)
        .write.csv(output_path, sep=delimiter, header=header, quoteAll=quote,
                   compression=compression)
    )

    # rename output files to desired name
    # this step removes the spark hash added to the name by default
    for filename in [f for f in list_dir(output_path) if f[0] != '.' and f != '_SUCCESS']:
        if num_files == 1 and output_file_name is not None:
            rename_file(output_path + filename, output_path + output_file_name)
        else:
            if output_file_name_template:
                template = output_file_name_template
            else:
                template = output_file_name_prefix + '{part_num}.gz'
            new_name = \
                template.format(part_num=re.match('''part-([0-9]+)[.-].*''', filename).group(1))
            rename_file(output_path + filename, output_path + new_name)


def custom_rename(spark, staging_dir, partition_name_prefix):
    """
    Unload custom data into partitions based on a date column
    """
    part_files_cmd = ['hadoop', 'fs', '-ls', '-R', staging_dir]

    part_files = subprocess.check_output(part_files_cmd).decode().strip().split("\n")

    spark.sparkContext.parallelize(part_files).repartition(1000).foreach(
        mk_move_file(partition_name_prefix)
    )


def s3distcp(src, dest,
             server_side_encryption=True,
             file_chunk_size=1000,
             deleteOnSuccess=True
             ):
    """Uses s3-dist-cp to copy files from s3 to s3.
    In the case that more than `file_chunk_size` files are to be copied, then these files will
    be uploaded in chunks. Where each chunk consists of no more than the value set
    in `file_chunk_size`. Otherwise, all files will be uploaded in a single operation.

    Args:
        dest (str): A URI that points to where the files should be copied to.
        src (str): A URI that points to the source directory that
            contains the files to be copied.
        server_side_encryption (bool, optional): Determines whether or
            not the copied files should be encrypted once they're copied
            to their target destination. Default is, Ture.
        deleteOnSuccess (bool, optional): Determines whether files should be
            deleted once they're copyied. Default is True.

            Note:
                server_side_encryption only works when copying files over to
                s3. In addition, the target bucket must require an encryption.
                If the above two requirements are not met, the command will
                fail.
        file_chunk_size (int, optional): The number of files to upload at once to s3.
            Default is, 5000.
    """

    if src and src[-1] != '/': src = src + '/'
    if dest[-1] != '/': dest = dest + '/'

    dist_cp_command = [
        's3-dist-cp',
        '--s3ServerSideEncryption',
        '--deleteOnSuccess',
        '--src', src,
        '--dest', dest
    ]

    if not deleteOnSuccess:
        dist_cp_command.remove('--deleteOnSuccess')

    try:
        files = [item[1] for item in list_files(path, recursive=recursive)]
    except:
        files = []

    if len(files) > file_chunk_size:
        if not server_side_encryption:
            dist_cp_command.remove('--s3ServerSideEncryption')

        write_manifests(files)
        file_names = list_manifest_files(OUTPUT_DIR)

        for file_name in file_names:
            subprocess.check_call(dist_cp_command + ['--srcPrefixesFile', file_name])

        clean_up_output_hdfs(''.join(['hdfs://', OUTPUT_DIR]))
    elif len(files) > 0:
        subprocess.check_call(dist_cp_command)


def timed_s3distcp(src, dest,
                 server_side_encryption=True,
                 file_chunk_size=5000,
                 deleteOnSuccess = True):
    """Uses s3-dist-cp to copy files from s3 to s3 and returns the commands runtime.
    In the case that more than `file_chunk_size` files are to be copied, then these files will
    be uploaded in chunks. Where each chunk consists of no more than the value set
    in `file_chunk_size`. Otherwise, all files will be uploaded in a single operation.

    Args:
        dest (str): A URI that points to where the files should be copied to.
        src (str): A URI that points to the source directory that
            contains the files to be copied.
        server_side_encryption (bool, optional): Determines whether or
            not the copied files should be encrypted once they're copied
            to their target destination. Default is, Ture.

            Note:
                server_side_encryption only works when copying files over to
                s3. In addition, the target bucket must require an encryption.
                If the above two requirements are not met, the command will
                fail.
        file_chunk_size (int, optional): The number of files to upload at once to s3.
            Default is, 5000.

    Returns:
        run_time (int): The total time it took for s3-dist-cp to run in seconds.
    """

    start = time.time()
    s3distcp(src, dest, server_side_encryption, file_chunk_size, deleteOnSuccess)

    return time.time() - start
