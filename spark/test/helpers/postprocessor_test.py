import pytest

import mock
import datetime

from pyspark.sql import Row

import spark.helpers.postprocessor as postprocessor


@pytest.mark.usefixtures("spark")
def test_trimmify(spark):
    "Ensure all string columns are trimmed"

    df = spark['spark'].sparkContext.parallelize([
        [' trim this'],
        ['trim this '],
        ['unchanged']
    ]).toDF()

    trimmed = postprocessor.trimmify(df).collect()

    for column in [el._1 for el in trimmed]:
        assert not column.startswith(' ') and not column.endswith(' ')


def test_nullify(spark):
    "Ensure all null columns are nullified"

    df = spark['spark'].sparkContext.parallelize([
        [None],
        ['NULL'],
        ['nUll'],
        ['this is also null'],
        ['NON NULL']
    ]).toDF()

    nullified_with_func = postprocessor.nullify(df, ['NULL', 'THIS IS ALSO NULL'], lambda c: c.upper() if c else None).collect()

    for (null_column, raw_column) in [
            (null_row._1, raw_row._1) for (null_row, raw_row) in zip(nullified_with_func, df.collect())
    ]:
        if not raw_column or raw_column.upper() in ['NULL', 'THIS IS ALSO NULL']:
            assert not null_column
        else:
            assert null_column


def test_apply_date_cap(spark):
    "Ensure specified date capping is applied"

    df = spark['spark'].sparkContext.parallelize([
        Row(row_id=1, date_col=datetime.date(2016, 1, 1)),
        Row(row_id=2, date_col=datetime.date(2016, 2, 1)),
        Row(row_id=3, date_col=datetime.date(2016, 3, 1)),
        Row(row_id=4, date_col=datetime.date(2016, 4, 1))
    ]).toDF()

    sample_date_cap = spark['spark'].sparkContext.parallelize([
        Row(gen_ref_1_dt=datetime.date(2016, 1, 15))
    ])

    old_sql_func = spark['sqlContext'].sql
    spark['sqlContext'].sql = mock.MagicMock(return_value=sample_date_cap)

    try:
        capped = postprocessor.apply_date_cap(spark['sqlContext'], 'date_col', '2016-03-15', '<feedid>', '<domain_name>')(df).collect()

        for row in capped:
            if row.row_id in [1, 4]:
                assert not row.date_col
            elif row.row_id == 2:
                assert row.date_col == '2016-02-01'
            elif row.row_id == 3:
                assert row.date_col == '2016-03-01'
    except:
        spark['sqlContext'].sql = old_sql_func
        raise

    spark['sqlContext'].sql = old_sql_func


def test_deobfuscate_hvid(spark):
    df = spark['spark'].sparkContext.parallelize([
        Row(row_id=1, hvid='100001'),
        Row(row_id=2, hvid='100001I')
    ]).toDF()

    assert postprocessor.deobfuscate_hvid('test_proj')(df).collect() \
        == [Row(row_id=1, hvid='1299049670'),
            Row(row_id=2, hvid=None)]


def test_apply_whitelist(spark):
    "Ensure specified whitelisting is applied"

    df = spark['spark'].sparkContext.parallelize([
        Row(row_id=1, whitelist_col='this value is ok'),
        Row(row_id=2, whitelist_col='this value is not ok'),
        Row(row_id=3, whitelist_col='this value is also not ok'),
        Row(row_id=4, whitelist_col='this value is neutral'),
        Row(row_id=5, whitelist_col='this value is a-ok')
    ]).toDF()

    sample_whitelist = spark['spark'].sparkContext.parallelize([
        Row(gen_ref_itm_nm='THIS VALUE IS OK'),
        Row(gen_ref_itm_nm='THIS VALUE IS A OK')
    ]).toDF()

    old_sql_func = spark['sqlContext'].sql
    spark['sqlContext'].sql = mock.MagicMock(return_value=sample_whitelist)

    try:
        whitelisted = postprocessor.apply_whitelist(spark['sqlContext'], 'whitelist_col', '<domain_name>')(df).collect()

        for row in whitelisted:
            if row.row_id in [2, 3, 4]:
                assert not row.whitelist_col
            elif row.row_id == 1:
                assert row.whitelist_col == 'THIS VALUE IS OK'
            elif row.row_id == 5:
                assert row.whitelist_col == 'THIS VALUE IS A OK'

    except:
        spark['sqlContext'].sql = old_sql_func
        raise

    spark['sqlContext'].sql = old_sql_func
