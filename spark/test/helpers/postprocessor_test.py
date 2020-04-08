import pytest

import mock
import datetime
from pyspark.sql.types import Row, StructType, StructField, StringType, DateType, IntegerType, DoubleType, FloatType, ArrayType
from pyspark.sql.functions import col, lit, udf
import spark.helpers.postprocessor as postprocessor
import spark.helpers.file_utils as file_utils

try:
    unicode
except:
    unicode = str

@pytest.mark.usefixtures("spark")
def test_trimmify(spark):
    "Ensure all string columns are trimmed and the schema is unchanged"
    
    # Include columns with various types for testing that schema is not changed
    schema_to_use = StructType([
        StructField('test_strings', StringType(), True),
        StructField('test_dates', DateType(), True),
        StructField('test_ints', IntegerType(), True),
        StructField('test_doubles', DoubleType(), True),
        StructField('test_floats', FloatType(), True),
        StructField('test_arrays', ArrayType(IntegerType()), True)
    ])    
    
    rdd = spark['spark'].sparkContext.parallelize([
        [' trim this', datetime.date(2016, 1, 1), 1, 1.1, 1.11, [1]],
        ['trim this ', datetime.date(2017, 1, 1), 2, 2.2, 2.22, [2]],
        ['unchanged', datetime.date(2018, 1, 1), 3, 3.3, 3.33, [3]],
    ])

    df = spark['spark'].createDataFrame(rdd, schema=schema_to_use)

    schema_before = df.schema
    trimmed = postprocessor.trimmify(df)
    schema_after = trimmed.schema  
    assert schema_before == schema_after

    trimmed = trimmed.collect()
    
    for column in [el['test_strings'] for el in trimmed]:
        assert not column.startswith(' ') and not column.endswith(' ')


def test_nullify(spark):
    "Ensure all null columns are nullified"
    
    # Include columns with various types for testing that schema is not changed
    schema_to_use = StructType([
        StructField('test_strings', StringType(), True),
        StructField('test_dates', DateType(), True),
        StructField('test_ints', IntegerType(), True),
        StructField('test_doubles', DoubleType(), True),
        StructField('test_floats', FloatType(), True),
        StructField('test_arrays', ArrayType(IntegerType()), True)
    ])

    rdd = spark['spark'].sparkContext.parallelize([
        [None, datetime.date(2012, 1, 1), 1, 1.1, 1.11, [1]],
        ['NULL', datetime.date(2013, 1, 1), 2, 2.2, 2.22, [2]],
        ['nUll', datetime.date(2014, 1, 1), 3, 3.3, 3.33, [3]],
        ['this is also null', datetime.date(2015, 1, 1), 4, 4.4, 4.44, [4]],
        ['NON NULL', datetime.date(2016, 1, 1), 5, 5.5, 5.55, [5]],
    ])

    df = spark['spark'].createDataFrame(rdd, schema=schema_to_use)
        
    schema_before = df.schema
    nullified_with_func = postprocessor.nullify(df, ['NULL', 'THIS IS ALSO NULL'], lambda c: c.upper() if c and type(c) is unicode else None)
    schema_after = nullified_with_func.schema
    assert schema_before == schema_after

    nullified_with_func = nullified_with_func.collect()
    
    for (null_column, raw_column) in [
            (null_row['test_strings'], raw_row['test_strings']) for (null_row, raw_row) in zip(nullified_with_func, df.collect())
    ]:
        if not raw_column or raw_column.upper() in ['NULL', 'THIS IS ALSO NULL']:
            assert not null_column
        else:
            assert null_column

def test_default_nullify(spark):
    "Ensure column's with 'NULL' are always nullified by default"
    
    rdd = spark['spark'].sparkContext.parallelize([
        ['NULL'],
        ['not null'],
        ['null'],
        ['Null']
    ])
    
    df = spark['spark'].createDataFrame(rdd)
    
    nullified_with_func = postprocessor.nullify(df)
    nullified_with_func = nullified_with_func.collect()

    assert not nullified_with_func[0]._1
    assert nullified_with_func[1]._1    
    assert not nullified_with_func[2]._1
    assert not nullified_with_func[3]._1

def test_apply_date_cap(spark):
    "Ensure specified date capping is applied"

    df = spark['spark'].sparkContext.parallelize([
        Row(row_id=1, date_col=datetime.date(2016, 1, 1)),
        Row(row_id=2, date_col=datetime.date(2016, 2, 1)),
        Row(row_id=3, date_col=datetime.date(2016, 3, 1)),
        Row(row_id=4, date_col=datetime.date(2016, 4, 1))
    ]).toDF()

    schema_before = df.schema

    sample_date_cap = spark['spark'].sparkContext.parallelize([
        Row(gen_ref_1_dt=datetime.date(2016, 1, 15))
    ])

    old_sql_func = spark['sqlContext'].sql
    spark['sqlContext'].sql = mock.MagicMock(return_value=sample_date_cap)

    try:
        capped = postprocessor.apply_date_cap(spark['sqlContext'], 'date_col', '2016-03-15', '<feedid>', '<domain_name>')(df)
        schema_after = capped.schema
        assert schema_before == schema_after

        capped = capped.collect()

        for row in capped:
            if row.row_id in [1, 4]:
                assert not row.date_col
            elif row.row_id == 2:
                assert row.date_col == datetime.date(2016, 2, 1)
            elif row.row_id == 3:
                assert row.date_col == datetime.date(2016, 3, 1)
    except:
        spark['sqlContext'].sql = old_sql_func
        raise

    spark['sqlContext'].sql = old_sql_func


def test_deobfuscate_hvid(spark):
    df = spark['spark'].sparkContext.parallelize([
        Row(row_id=1, hvid='100001'),
        Row(row_id=2, hvid='100001I')
    ]).toDF()

    schema_before = df.schema

    deobfuscated_hvid = postprocessor.deobfuscate_hvid('test_proj')(df)
    schema_after = deobfuscated_hvid.schema

    assert schema_before == schema_after

    assert deobfuscated_hvid.collect() \
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
    
    schema_before = df.schema    

    sample_whitelist = spark['spark'].sparkContext.parallelize([
        Row(gen_ref_itm_nm='THIS VALUE IS OK'),
        Row(gen_ref_itm_nm='THIS VALUE IS A OK')
    ]).toDF()

    old_sql_func = spark['sqlContext'].sql
    spark['sqlContext'].sql = mock.MagicMock(return_value=sample_whitelist)

    try:
        whitelisted = postprocessor.apply_whitelist(spark['sqlContext'], 'whitelist_col', '<domain_name>')(df)
        schema_after = whitelisted.schema
        assert schema_before == schema_after

        whitelisted = whitelisted.collect()
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


def test_add_input_filename(spark):
    script_path = __file__
    df = spark['sqlContext'].read.csv(file_utils.get_abs_path(script_path, './resources/input_filename.txt.aa.txt'))

    with_filename = postprocessor.add_input_filename('source_file_name')(df)
    with_filename_parent_dir = postprocessor.add_input_filename('source_file_name', True)(df)

    for row in with_filename.collect():
        assert row.source_file_name == 'input_filename.txt'

    for row in with_filename_parent_dir.collect():
        assert row.source_file_name == "file://" + file_utils.get_abs_path(script_path, './resources/input_filename.txt')


def test_add_null_column(spark):
    df = spark['spark'].sparkContext.parallelize([
        Row(row_id=1),
        Row(row_id=2),
        Row(row_id=3),
        Row(row_id=4),
        Row(row_id=5)
    ]).toDF()
    df = postprocessor.add_null_column('test_col')(df)

    table_row_count = df.select().count()
    null_column_count = df.select('test_col').where(col('test_col').isNull()).count()

    assert 'test_col' in df.columns
    assert 'row_id' in df.columns
    assert null_column_count == table_row_count
    assert table_row_count == 5
