import pytest

from functools import reduce
import os
import shutil

import spark.helpers.file_utils as file_utils
import spark.helpers.normalized_records_unloader as normalized_records_unloader

from datetime import datetime

test_staging_dir  = file_utils.get_abs_path(
    __file__, './test-staging/'
) + '/'
test_staging_dir2 = file_utils.get_abs_path(
    __file__, './test-staging2/'
) + '/'
test_staging_dir3 = file_utils.get_abs_path(
    __file__, './test-staging3/'
) + '/'
prefix = 'PREFIX'

HVM_HISTORICAL_DATE = datetime(2015, 01, 01)


@pytest.mark.usefixtures("spark")
def test_init(spark):
    spark['sqlContext'].sql('DROP TABLE IF EXISTS lab_common_model')
    spark['runner'].run_spark_script('../../common/lab_common_model.sql', [
        ['table_name', 'lab_common_model', False],
        ['properties', '', False]
    ])

    column_count = None
    with open(file_utils.get_abs_path(
            __file__, '../../common/lab_common_model.sql'
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

    assert set(date_partition) == set(['part_best_date=2015-11', 'part_best_date=1992-11', 'part_best_date=NULL'])


def test_prefix():
    "Ensure prefix was added to part files"
    part_files = filter(
        lambda f: not f.endswith('.crc'),
        os.listdir(
            test_staging_dir + '/part_provider=test_provider/part_best_date=NULL/'
        )
    )
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
            test_staging_dir + '/part_provider=test_provider2/part_best_date=NULL/'
        )
    )
    for f in part_files:
        assert f.startswith(prefix)

    # ensure old data was not re-prefixed
    part_files = filter(
        lambda f: not f.endswith('.crc'),
        os.listdir(
            test_staging_dir + '/part_provider=test_provider/part_best_date=NULL/'
        )
    )
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
            test_staging_dir + '/part_provider=test_provider/part_best_date=NULL/'
        )
    )

    # assert that old prefixes were unchanged
    assert filter(
        lambda f: f.startswith(prefix + '_part'),
        os.listdir(
            test_staging_dir + '/part_provider=test_provider/part_best_date=NULL/'
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


def test_cleanup():
    shutil.rmtree(test_staging_dir)
    shutil.rmtree(test_staging_dir2)
    shutil.rmtree(test_staging_dir3)
