import pytest

from pyspark.sql.types import StructField, StructType, StringType, Row

import spark.qa.checks.utils as checks_utils

@pytest.mark.usefixtures("spark")
def test_remove_nulls_and_blanks(spark):
    test_df = spark['spark'].sparkContext.parallelize([
        [''], [None], ['SOMETHING'], [' ']
    ]).toDF(StructType([
        StructField('col1', StringType())
    ]))
    assert checks_utils.remove_nulls_and_blanks(test_df, 'col1').collect() \
        == [Row('SOMETHING')]


def test_all_values_in_src_and_target(spark):
    src = spark['spark'].sparkContext.parallelize([
        ['claim-1'],
        ['claim-2'],
        ['claim-3']
    ]).toDF(StructType([
        StructField('col1', StringType())
    ]))

    target = spark['spark'].sparkContext.parallelize([
        ['claim-1'],
        ['claim-2'],
        ['claim-2'],
        ['claim-3']
    ]).toDF(StructType([
        StructField('col1', StringType())
    ]))

    target_broken = spark['spark'].sparkContext.parallelize([
        ['claim-1'],
        ['claim-2']
    ]).toDF(StructType([
        StructField('col1', StringType())
    ]))

    # this works
    checks_utils.assert_all_values_in_src_and_target(src, target, 'col1', 'col1', 'COLUMN')

    # this fails
    with pytest.raises(AssertionError) as assertion_error:
        checks_utils.assert_all_values_in_src_and_target(src, target_broken, 'col1', 'col1', 'COLUMN')

        assert assertion_error.value.message == 'COLUMN values in source did not exist in target. Examples: claim-3'


def test_full_fill_in_target(spark):
    broken = spark['spark'].sparkContext.parallelize([
        ['claim-1'],
        [None],
        ['claim-3']
    ]).toDF(StructType([
        StructField('col1', StringType())
    ]))

    broken_blank = spark['spark'].sparkContext.parallelize([
        ['claim-1'],
        [' '],
        ['claim-3']
    ]).toDF(StructType([
        StructField('col1', StringType())
    ]))

    working = spark['spark'].sparkContext.parallelize([
        ['claim-1'],
        ['claim-2'],
        ['claim-3']
    ]).toDF(StructType([
        StructField('col1', StringType())
    ]))

    checks_utils.assert_full_fill_in_target(working, 'col1')

    with pytest.raises(AssertionError):
        checks_utils.assert_full_fill_in_target(broken, 'col1')

    with pytest.raises(AssertionError):
        checks_utils.assert_full_fill_in_target(broken_blank, 'col1')
