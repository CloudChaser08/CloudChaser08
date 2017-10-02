import pytest
import spark.helpers.privacy.common as common_priv
from pyspark.sql.functions import upper
from pyspark.sql.types import StructField, StructType, StringType, Row

@pytest.mark.usefixtures("spark")
def test_transform(spark):

    example_transformer = {
        'col1': {
            'func': upper,
            'args': ['col1'],
            'built-in': True
        },
        'col2': {
            'func': lambda c2, c1: c1 + '_' + c2,
            'args': ['col2', 'col1']
        }
    }

    # get transformer function
    transformer_func = common_priv._transform(example_transformer)

    test_df = spark['spark'].sparkContext.parallelize([
        ['val1', 'val2', 'val3']
    ]).toDF(StructType([
        StructField('col1', StringType()),
        StructField('col2', StringType()),
        StructField('col3', StringType())
    ]))

    # built in `upper` transformation
    assert test_df.select(transformer_func('col1')).collect() \
        == [Row('VAL1')]

    # custom function transformation
    assert test_df.select(transformer_func('col2')).collect() \
        == [Row('val1_val2')]

    # col3 has no transformations defined
    assert test_df.select(transformer_func('col3')).collect() \
        == [Row('val3')]


def test_filter(spark):
    # test df including commonly filtered fields
    test_df = spark['spark'].sparkContext.parallelize([
        ['100', '1880', '2017-01-01', 'dummyval']
    ]).toDF(StructType([
        StructField('patient_age', StringType()),
        StructField('patient_year_of_birth', StringType()),
        StructField('date_service', StringType()),
        StructField('notransform', StringType()),
    ]))

    # assertion with no additional transforms
    assert common_priv.filter(test_df).collect() \
        == [Row('90', '1927', '2017-01-01', 'dummyval')]

    # save original state of built-in transformer
    old_transformer = dict(common_priv.column_transformer)

    # assertion including additional transforms
    assert common_priv.filter(test_df, {
        'notransform': {
            'func': upper,
            'args': ['notransform'],
            'built-in': True
        }
    }).collect() == [Row('90', '1927', '2017-01-01', 'DUMMYVAL')]

    # assert original transformer was not modified by additional
    # transforms dict update
    assert common_priv.column_transformer == old_transformer
