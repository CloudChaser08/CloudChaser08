import pytest

from functools import reduce
import os
import shutil

import spark.helpers.file_utils as file_utils
import spark.helpers.normalized_records_unloader as normalized_records_unloader

from datetime import datetime

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


def cleanup():
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


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup()
    spark['sqlContext'].sql('DROP TABLE IF EXISTS lab_common_model')
    spark['runner'].run_spark_script('../../common/lab_common_model.sql', [
        ['table_name', 'lab_common_model', False],
        ['properties', '', False]
    ])

    column_count = None
    with open(file_utils.get_abs_path(
            script_path, '../../common/lab_common_model.sql'
    ), 'r') as lab:
        column_count = len(lab.readlines()) - 6

    # insert 50 values after HVM_HISTORICAL_DATE
    # Note: 13 is the column number of 'date_service'
    spark['sqlContext'].sql(
        'INSERT INTO lab_common_model VALUES {}'
        .format(reduce(
            lambda x1, x2: x1 + ', ' + x2,
            map(
                lambda l: l + reduce(
                    lambda str1, str2: str1 + str2,
                    [',NULL' if i != 13 else ',CAST("2015-11-07" AS DATE)' for i in range(column_count)]
                ) + ')',
                ['({}'.format(x) for x in range(50)]
            )
        ))
    )

    # insert 50 values before HVM_HISTORICAL_DATE
    # Note: 13 is the column number of 'date_service'
    spark['sqlContext'].sql(
        'INSERT INTO lab_common_model VALUES {}'
        .format(reduce(
            lambda x1, x2: x1 + ', ' + x2,
            map(
                lambda l: l + reduce(
                    lambda str1, str2: str1 + str2,
                    [',NULL' if i != 13 else ',CAST("1992-11-07" AS DATE)' for i in range(column_count)]
                ) + ')',
                ['({}'.format(x) for x in range(50)]
            )
        ))
    )

    # insert 100 values with NULL date_service into the table
    spark['sqlContext'].sql(
        'INSERT INTO lab_common_model VALUES {}'
        .format(reduce(
            lambda x1, x2: x1 + ', ' + x2,
            map(
                lambda l: l + reduce(
                    lambda str1, str2: str1 + str2,
                    [',NULL' for _ in range(column_count)]
                ) + ')',
                ['({}'.format(x) for x in range(100)]
            )
        ))
    )

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
        spark['spark'], spark['runner'], spark['sqlContext'].sql('select * from lab_common_model'),
        'date_service', prefix, 'test_provider', test_dir=test_staging_dir_unload
    )
    normalized_records_unloader.unload(
        spark['spark'], spark['runner'], spark['sqlContext'].sql('select * from lab_common_model'),
        'date_service', prefix, 'test_provider', test_dir=test_staging_dir_unload2,
        date_partition_value='2017-01'
    )
    normalized_records_unloader.unload(
        spark['spark'], spark['runner'], spark['sqlContext'].sql('select * from lab_common_model'),
        'date_service', prefix, 'test_provider', hvm_historical_date=HVM_HISTORICAL_DATE,
        test_dir=test_staging_dir_unload3
    )


def test_correct_dynamic_partitions():
    "Ensure correct dynamic partitions were created"
    provider_partition = filter(
        lambda f: "hive-staging" not in f,
        os.listdir(test_staging_dir)
    )
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
    part_files = filter(
        lambda f: not f.endswith('.crc'),
        os.listdir(
            test_staging_dir + '/part_provider=test_provider/part_best_date=0_PREDATES_HVM_HISTORY/'
        )
    )
    for f in part_files:
        assert f.startswith(prefix)


def test_prefix_unload():
    "Ensure prefix was added to part files created by the 'unload' function"
    part_files = filter(
        lambda f: not f.endswith('.crc'),
        os.listdir(
            test_staging_dir_unload + '/part_provider=test_provider/part_best_date=0_PREDATES_HVM_HISTORY/'
        )
    )
    assert part_files
    for f in part_files:
        assert f.startswith(prefix)


def test_fixed_partition():
    "Ensure that when a fixed partition name is specific, only that partition is created"
    provider_partition = filter(
        lambda f: "hive-staging" not in f,
        os.listdir(test_staging_dir2)
    )
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
    part_files = filter(
        lambda f: not f.endswith('.crc'),
        os.listdir(
            test_staging_dir + '/part_provider=test_provider2/part_best_date=0_PREDATES_HVM_HISTORY/'
        )
    )
    for f in part_files:
        assert f.startswith(prefix)

    # ensure old data was not re-prefixed
    part_files = filter(
        lambda f: not f.endswith('.crc'),
        os.listdir(
            test_staging_dir + '/part_provider=test_provider/part_best_date=0_PREDATES_HVM_HISTORY/'
        )
    )
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
    part_files = filter(
        lambda f: not f.endswith('.crc'),
        os.listdir(
            test_staging_dir_unload + '/part_provider=test_provider2/part_best_date=0_PREDATES_HVM_HISTORY/'
        )
    )
    assert part_files
    for f in part_files:
        assert f.startswith(prefix)

    # ensure old data was not re-prefixed
    part_files = filter(
        lambda f: not f.endswith('.crc'),
        os.listdir(
            test_staging_dir_unload + '/part_provider=test_provider/part_best_date=0_PREDATES_HVM_HISTORY/'
        )
    )
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
    assert filter(
        lambda f: f.startswith(prefix + '_NEW_part'),
        os.listdir(
            test_staging_dir + '/part_provider=test_provider/part_best_date=0_PREDATES_HVM_HISTORY/'
        )
    )

    # assert that old prefixes were unchanged
    assert filter(
        lambda f: f.startswith(prefix + '_part'),
        os.listdir(
            test_staging_dir + '/part_provider=test_provider/part_best_date=0_PREDATES_HVM_HISTORY/'
        )
    )


def test_unload_new_prefix_unload(spark):
    "Ensure that a new prefix can be unloaded to an existing partition using the 'unload' function"
    normalized_records_unloader.unload(
        spark['spark'], spark['runner'], spark['sqlContext'].sql('select * from lab_common_model'),
        'date_service', prefix + '_NEW', 'test_provider', test_dir=test_staging_dir_unload
    )

    # assert that new prefixes were added
    assert filter(
        lambda f: f.startswith(prefix + '_NEW_part'),
        os.listdir(
            test_staging_dir_unload + '/part_provider=test_provider/part_best_date=0_PREDATES_HVM_HISTORY/'
        )
    )

    # assert that old prefixes were unchanged
    assert filter(
        lambda f: f.startswith(prefix + '_part'),
        os.listdir(
            test_staging_dir_unload + '/part_provider=test_provider/part_best_date=0_PREDATES_HVM_HISTORY/'
        )
    )


def test_hvm_historical_date(spark):
    "Ensure correct dynamic partitions were created"
    provider_partition = filter(
        lambda f: "hive-staging" not in f,
        os.listdir(test_staging_dir3)
    )
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
    with gzip.open(test_staging_dir4 + filename) as decompressed:
        assert [line.split('|') for line in decompressed.read().splitlines()] \
            == [['"val1"', '"val2"'], ['"val3"', '"val4"']]

    normalized_records_unloader.unload_delimited_file(
        spark['spark'], spark['runner'], test_staging_dir4, 'test_table', test=True, file_name='my_file.gz'
    )

    filename = [f for f in os.listdir(test_staging_dir4) if f.endswith('.gz')][0]

    assert filename == 'my_file.gz'

    with gzip.open(test_staging_dir4 + filename) as decompressed:
        assert [line.split('|') for line in decompressed.read().splitlines()] \
            == [['"val1"', '"val2"'], ['"val3"', '"val4"']]


def test_cleanup():
    cleanup()
