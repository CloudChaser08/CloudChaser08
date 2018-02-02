import pytest
from spark.helpers.privacy.common import Transformer
import spark.helpers.privacy.emr.common as common_emr_priv
from pyspark.sql.functions import upper
from pyspark.sql.types import StructField, StructType, StringType, Row


@pytest.mark.usefixtures("spark")
def test_filter(spark):
    # test df including commonly filtered fields
    test_df = spark['spark'].sparkContext.parallelize([
        ['100', '1880', '2017-01-01', 'dummyval']
    ]).toDF(StructType([
        StructField('ptnt_age_num', StringType()),
        StructField('ptnt_birth_yr', StringType()),
        StructField('enc_dt', StringType()),
        StructField('notransform', StringType())
    ]))

    # assertion with no additional transforms
    assert common_emr_priv.filter(test_df).collect() \
        == [Row('90', '1927', '2017-01-01', 'dummyval')]

    # save original state of built-in transformer
    old_transformer = Transformer(**dict(common_emr_priv.emr_transformer.transforms))

    # assertion including additional transforms
    assert common_emr_priv.filter(test_df, Transformer(
        notransform={
            'func': [upper],
            'args': [['notransform']],
            'built-in': [True]
        }
    )).collect() == [Row('90', '1927', '2017-01-01', 'DUMMYVAL')]

    # assert original transformer was not modified by additional
    # transforms dict update
    assert common_emr_priv.emr_transformer.transforms == old_transformer.transforms
