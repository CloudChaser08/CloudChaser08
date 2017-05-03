import pytest

from functools import reduce
import os
import shutil

import spark.helpers.file_utils as file_utils
import spark.helpers.normalized_records_unloader as normalized_records_unloader

test_staging_dir = file_utils.get_rel_path(
    __file__, './test-staging/'
)
prefix = 'PREFIX'


@pytest.mark.usefixtures("spark")
def test_init(spark):
    spark['sqlContext'].sql('DROP TABLE IF EXISTS lab_common_model')
    spark['runner'].run_spark_script(file_utils.get_rel_path(
        __file__,
        '../../common/lab_common_model.sql'
    ), [
        ['table_name', 'lab_common_model', False],
        ['properties', '', False]
    ])

    column_count = None
    with open(file_utils.get_rel_path(
            __file__, '../../common/lab_common_model.sql'
    ), 'r') as lab:
        column_count = len(lab.readlines()) - 6

    # insert 100 values into the table
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
        test_staging_dir
    )


def test_correct_partitions():
    "Ensure correct partitions were created"
    provider_partition = filter(
        lambda f: "hive-staging" not in f,
        os.listdir(test_staging_dir)
    )
    assert provider_partition == ['part_provider=test_provider']

    date_partition = os.listdir(
        test_staging_dir + '/part_provider=test_provider/'
    )

    assert date_partition == ['part_best_date=NULL']


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


def test_cleanup():
    shutil.rmtree(test_staging_dir)
