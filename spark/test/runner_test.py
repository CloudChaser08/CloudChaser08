import pytest
import os
from pyspark.sql import Row

(df1, df2) = (None, None)


def cleanup(spark):
    spark['runner'].unpersist('df')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)

    global df1, df2, persisted_df1, persisted_df2
    data_row = Row('col1', 'col2', 'col3')
    df1 = spark['spark'].sparkContext.parallelize([
        Row('1', '2', '3'),
        Row('a', 'b', 'c'),
        Row('x', 'y', 'z')
    ]).toDF()

    df2 = spark['spark'].sparkContext.parallelize([
        Row('3', '2', '1'),
        Row('c', 'b', 'a'),
        Row('z', 'y', 'x')
    ]).toDF()


def test_persisted(spark):
    """
        Test that df2 is indeed persisted using the default MEMORY_AND_DISK
        strategy
    """
    df2.persist_and_track('df')
    assert df2.storageLevel.useDisk and df2.storageLevel.useMemory \
            and not df2.storageLevel.deserialized and df2.storageLevel.replication == 1


def test_unpersisted(spark):
    """
        Test that df1 was unpersisted because it used the same identifier as df2
    """
    df1.persist_and_track('df')
    df2.persist_and_track('df')
    assert not df1.storageLevel.useDisk and not df1.storageLevel.useMemory \
            and not df1.storageLevel.deserialized and not df1.storageLevel.useOffHeap


def test_unpersisting(spark):
    """
        Test that we can unpersist df2 without using its explicit handle
    """

    df2.persist_and_track('df')
    spark['runner'].unpersist('df')
    assert not df2.storageLevel.useDisk and not df2.storageLevel.useMemory \
            and not df2.storageLevel.deserialized and not df2.storageLevel.useOffHeap


def test_run_all_scripts(spark):
    """
        Test that if sql scripts follow the correct naming convention, they
        are all run in sequence and the results of the last one are returned
    """
    scripts_dir = os.path.dirname(__file__) + '/resources/'
    res = spark['runner'].run_all_spark_scripts(directory_path=scripts_dir).collect()

    assert res[0][0] == 5
