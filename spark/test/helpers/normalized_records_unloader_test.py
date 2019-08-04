import pytest
import os
import shutil

import spark.helpers.file_utils as file_utils
import spark.helpers.normalized_records_unloader as normalized_records_unloader

from datetime import datetime
from functools import reduce

from pyspark.sql.utils import AnalysisException

script_path = __file__
test_staging_dir  = file_utils.get_abs_path(
    script_path, './test-staging/'
) + '/'
test_staging_dir2 = file_utils.get_abs_path(
    script_path, './test-staging2/'
) + '/'
test_staging_dir3 = file_utils.get_abs_path(
    script_path, './test-staging3/'
) + '/'
test_staging_dir4 = file_utils.get_abs_path(
    script_path, './test-staging4/'
) + '/'

test_staging_dir_unload = file_utils.get_abs_path(
    script_path, './test-staging-unload/'
) + '/'
test_staging_dir_unload2 = file_utils.get_abs_path(
    script_path, './test-staging-unload2/'
) + '/'
test_staging_dir_unload3 = file_utils.get_abs_path(
    script_path, './test-staging-unload3/'
) + '/'

prefix = 'PREFIX'

HVM_HISTORICAL_DATE = datetime(2015, 1, 1)


def cleanup(spark):
    try:
        shutil.rmtree(test_staging_dir)
    except:
        pass
    try:
        shutil.rmtree(test_staging_dir2)
    except:
        pass
    try:
        shutil.rmtree(test_staging_dir3)
    except:
        pass
    try:
        shutil.rmtree(test_staging_dir4)
    except:
        pass
    try:
        shutil.rmtree(test_staging_dir_unload)
    except:
        pass
    try:
        shutil.rmtree(test_staging_dir_unload2)
    except:
        pass
    try:
        shutil.rmtree(test_staging_dir_unload3)
    except:
        pass
    try:
        spark['sqlContext'].sql('DROP VIEW IF EXISTS lab_common_model')
    except AnalysisException:
        pass
    try:
        spark['sqlContext'].sql('DROP TABLE IF EXISTS lab_common_model')
    except AnalysisException:
        pass

# Note: 13 is the column number of 'date_service'
def make_lab_model_df(sqlContext, date_service, row_count):
    schema = sqlContext.table('lab_common_model').schema

    df_data = []
    for i in range(row_count):
        d = [None] * len(schema)
        d[0] = i
        d[14] = date_service
        df_data.append(tuple(d))

    return sqlContext.createDataFrame(df_data, schema)

@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)
    spark['sqlContext'].sql('DROP TABLE IF EXISTS lab_common_model')
    spark['runner'].run_spark_script('../../common/lab_common_model.sql', [
        ['table_name', 'lab_common_model', False],
        ['properties', '', False]
    ])

    # 50 rows after HVM_HISTORICAL_DATE
    after_historical = make_lab_model_df(spark['sqlContext'], datetime(2015, 11, 7), 50)

    # 50 row before HVM_HISTORICAL_DATE
    before_historical = make_lab_model_df(spark['sqlContext'], datetime(1992, 11, 7), 50)

    # 100 rows with NULL date_service
    null_dates = make_lab_model_df(spark['sqlContext'], None, 50)

    after_historical.union(before_historical).union(null_dates) \
            .cache_and_track('lab_common_model') \
            .createOrReplaceTempView('lab_common_model')

    normalized_records_unloader.partition_and_rename(
        spark['spark'], spark['runner'], 'lab', 'lab_common_model.sql',
        'test_provider', 'lab_common_model', 'date_service', prefix,
        test_dir=test_staging_dir
    )
    normalized_records_unloader.partition_and_rename(
        spark['spark'], spark['runner'], 'lab', 'lab_common_model.sql',
        'test_provider', 'lab_common_model', 'date_service', prefix,
        partition_value='2017-01', test_dir=test_staging_dir2
    )
    normalized_records_unloader.partition_and_rename(
        spark['spark'], spark['runner'], 'lab', 'lab_common_model.sql',
        'test_provider', 'lab_common_model', 'date_service', prefix,
        hvm_historical_date=HVM_HISTORICAL_DATE, test_dir=test_staging_dir3
    )

    normalized_records_unloader.unload(
        spark['spark'], spark['runner'], spark['sqlContext'].table('lab_common_model'),
        'date_service', prefix, 'test_provider', test_dir=test_staging_dir_unload
    )
    normalized_records_unloader.unload(
        spark['spark'], spark['runner'], spark['sqlContext'].table('lab_common_model'),
        'date_service', prefix, 'test_provider', test_dir=test_staging_dir_unload2,
        date_partition_value='2017-01'
    )
    normalized_records_unloader.unload(
        spark['spark'], spark['runner'], spark['sqlContext'].table('lab_common_model'),
        'date_service', prefix, 'test_provider', hvm_historical_date=HVM_HISTORICAL_DATE,
        test_dir=test_staging_dir_unload3
    )


def test_correct_dynamic_partitions():
    "Ensure correct dynamic partitions were created"
    provider_partition = [f for f in os.listdir(test_staging_dir) if "hive-staging" not in f]
    assert provider_partition == ['part_provider=test_provider']

    date_partition = os.listdir(
        test_staging_dir + '/part_provider=test_provider/'
    )

    assert set(date_partition) == set(['part_best_date=2015-11', 'part_best_date=1992-11', 'part_best_date=0_PREDATES_HVM_HISTORY'])


def test_correct_dynamic_partitions_unload():
    "Ensure correct dynamic partitions were created by the 'unload' function"
    provider_partition = [
        f for f in os.listdir(test_staging_dir_unload)
        if 'hive-staging' not in f and 'SUCCESS' not in f
    ]
    assert provider_partition == ['part_provider=test_provider']

    date_partition = os.listdir(
        test_staging_dir_unload + '/part_provider=test_provider/'
    )

    assert set(date_partition) == set(['part_best_date=2015-11', 'part_best_date=1992-11', 'part_best_date=0_PREDATES_HVM_HISTORY'])


def test_prefix():
    "Ensure prefix was added to part files"
    part_files = [f for f in os.listdir(
            test_staging_dir + '/part_provider=test_provider/part_best_date=0_PREDATES_HVM_HISTORY/'
        ) if not f.endswith('.crc')]
    for f in part_files:
        assert f.startswith(prefix)


def test_prefix_unload():
    "Ensure prefix was added to part files created by the 'unload' function"
    part_files = [f for f in os.listdir(
            test_staging_dir_unload + '/part_provider=test_provider/part_best_date=0_PREDATES_HVM_HISTORY/'
        ) if not f.endswith('.crc')]
    assert part_files
    for f in part_files:
        assert f.startswith(prefix)


def test_fixed_partition():
    "Ensure that when a fixed partition name is specific, only that partition is created"
    provider_partition = [f for f in os.listdir(test_staging_dir2) if "hive-staging" not in f]
    assert provider_partition == ['part_provider=test_provider']

    date_partition = os.listdir(
        test_staging_dir2 + '/part_provider=test_provider/'
    )

    assert date_partition == ['part_best_date=2017-01']


def test_fixed_partition_unload():
    """
    Ensure that when a fixed partition name is specific, only that
    partition is created when using the 'unload' function
    """
    provider_partition = [
        f for f in os.listdir(test_staging_dir_unload)
        if 'hive-staging' not in f and 'SUCCESS' not in f
    ]
    assert provider_partition == ['part_provider=test_provider']

    date_partition = os.listdir(
        test_staging_dir_unload2 + '/part_provider=test_provider/'
    )

    assert date_partition == ['part_best_date=2017-01']


def test_unload_separate_provider(spark):
    """
    Ensure that when a separate provider is unloaded to the same staging
    dir, nothing gets double-prefixed
    """
    normalized_records_unloader.partition_and_rename(
        spark['spark'], spark['runner'], 'lab', 'lab_common_model.sql',
        'test_provider2', 'lab_common_model', 'date_service', prefix,
        test_dir=test_staging_dir
    )

    # ensure new data was addeed
    assert os.listdir(test_staging_dir + 'part_provider=test_provider2/')

    # ensure new data was prefixed
    part_files = [f for f in os.listdir(
            test_staging_dir + '/part_provider=test_provider2/part_best_date=0_PREDATES_HVM_HISTORY/'
        ) if not f.endswith('.crc')]
    for f in part_files:
        assert f.startswith(prefix)

    # ensure old data was not re-prefixed
    part_files = [f for f in os.listdir(
            test_staging_dir + '/part_provider=test_provider/part_best_date=0_PREDATES_HVM_HISTORY/'
        ) if not f.endswith('.crc')]
    for f in part_files:
        assert f.startswith('{}_part'.format(prefix))


def test_unload_separate_provider_unload(spark):
    """
    Ensure that when a separate provider is unloaded to the same staging
    dir using the 'unload' function, nothing gets double-prefixed
    """
    normalized_records_unloader.unload(
        spark['spark'], spark['runner'], spark['sqlContext'].sql('select * from lab_common_model'),
        'date_service', prefix, 'test_provider2', test_dir=test_staging_dir_unload
    )

    # ensure new data was addeed
    assert os.listdir(test_staging_dir_unload + 'part_provider=test_provider2/')

    # ensure new data was prefixed
    part_files = [f for f in os.listdir(
            test_staging_dir_unload + '/part_provider=test_provider2/part_best_date=0_PREDATES_HVM_HISTORY/'
        ) if not f.endswith('.crc')]
    assert part_files
    for f in part_files:
        assert f.startswith(prefix)

    # ensure old data was not re-prefixed
    part_files = [f for f in os.listdir(
            test_staging_dir_unload + '/part_provider=test_provider/part_best_date=0_PREDATES_HVM_HISTORY/'
        ) if not f.endswith('.crc')]
    assert part_files
    for f in part_files:
        assert f.startswith('{}_part'.format(prefix))


def test_unload_new_prefix(spark):
    "Ensure that a new prefix can be unloaded to an existing partition"
    normalized_records_unloader.partition_and_rename(
        spark['spark'], spark['runner'], 'lab', 'lab_common_model.sql',
        'test_provider', 'lab_common_model', 'date_service', prefix + '_NEW',
        test_dir=test_staging_dir
    )

    # assert that new prefixes were added
    assert [f for f in os.listdir(
            test_staging_dir + '/part_provider=test_provider/part_best_date=0_PREDATES_HVM_HISTORY/'
        ) if f.startswith(prefix + '_NEW_part')]

    # assert that old prefixes were unchanged
    assert [f for f in os.listdir(
            test_staging_dir + '/part_provider=test_provider/part_best_date=0_PREDATES_HVM_HISTORY/'
        ) if f.startswith(prefix + '_part')]


def test_unload_new_prefix_unload(spark):
    "Ensure that a new prefix can be unloaded to an existing partition using the 'unload' function"
    normalized_records_unloader.unload(
        spark['spark'], spark['runner'], spark['sqlContext'].sql('select * from lab_common_model'),
        'date_service', prefix + '_NEW', 'test_provider', test_dir=test_staging_dir_unload
    )

    # assert that new prefixes were added
    assert [f for f in os.listdir(
            test_staging_dir_unload + '/part_provider=test_provider/part_best_date=0_PREDATES_HVM_HISTORY/'
        ) if f.startswith(prefix + '_NEW_part')]

    # assert that old prefixes were unchanged
    assert [f for f in os.listdir(
            test_staging_dir_unload + '/part_provider=test_provider/part_best_date=0_PREDATES_HVM_HISTORY/'
        ) if f.startswith(prefix + '_part')]


def test_hvm_historical_date(spark):
    "Ensure correct dynamic partitions were created"
    provider_partition = [f for f in os.listdir(test_staging_dir3) if "hive-staging" not in f]
    assert provider_partition == ['part_provider=test_provider']

    date_partition = os.listdir(
        test_staging_dir3 + '/part_provider=test_provider/'
    )

    assert set(date_partition) == set(['part_best_date=2015-11', 'part_best_date=0_PREDATES_HVM_HISTORY'])


def test_hvm_historical_date_unload(spark):
    "Ensure hvm historical date is used as a secondary isNull filter when using the 'unload' function"
    provider_partition = [
        f for f in os.listdir(test_staging_dir_unload3)
        if 'hive-staging' not in f and 'SUCCESS' not in f
    ]
    assert provider_partition == ['part_provider=test_provider']

    date_partition = os.listdir(
        test_staging_dir_unload3 + '/part_provider=test_provider/'
    )

    assert set(date_partition) == set(['part_best_date=2015-11', 'part_best_date=0_PREDATES_HVM_HISTORY'])


def test_unload_delimited_file(spark):
    # create test table
    spark['spark'].sparkContext.parallelize([
        ['val1', 'val2'],
        ['val3', 'val4'],
    ]).toDF().createOrReplaceTempView('test_table')

    normalized_records_unloader.unload_delimited_file(
        spark['spark'], spark['runner'], test_staging_dir4, 'test_table', test=True
    )

    filename = [f for f in os.listdir(test_staging_dir4) if f.endswith('.gz')][0]

    import gzip
    with gzip.open(test_staging_dir4 + filename, 'rt') as decompressed:
        assert [line.split('|') for line in decompressed.read().splitlines()] \
            == [['"val1"', '"val2"'], ['"val3"', '"val4"']]

    normalized_records_unloader.unload_delimited_file(
        spark['spark'], spark['runner'], test_staging_dir4, 'test_table', test=True, output_file_name='my_file.gz'
    )

    filename = [f for f in os.listdir(test_staging_dir4) if f.endswith('.gz')][0]

    assert filename == 'my_file.gz'

    with gzip.open(test_staging_dir4 + filename, 'rt') as decompressed:
        assert [line.split('|') for line in decompressed.read().splitlines()] \
            == [['"val1"', '"val2"'], ['"val3"', '"val4"']]


@pytest.mark.usefixtures("spark")
def test_cleanup(spark):
    cleanup(spark)
