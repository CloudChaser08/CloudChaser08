import pytest
from spark.helpers.privacy.common import Transformer, TransformFunction
import spark.helpers.privacy.emr.common as common_emr_priv
from pyspark.sql.functions import upper
from pyspark.sql.types import StructField, StructType, StringType, Row


def filter_helper(spark, vals, additional_transformer=None): 
    test_df = spark['spark'].sparkContext.parallelize([vals])\
		.toDF(StructType([
		    StructField('ptnt_age_num', StringType()),
		    StructField('ptnt_birth_yr', StringType()),
		    StructField('ptnt_gender_cd', StringType()),
		    StructField('enc_dt', StringType()),
		    StructField('notransform', StringType())
		]))
    return common_emr_priv.filter(test_df, additional_transformer).collect() 


@pytest.mark.usefixtures("spark")
def test_filter(spark):
    
    # test df including commonly filtered fields with no additional transformations
    filtered_df = filter_helper(spark, ['100', '1880', 'M', '2017-01-01', 'dummyval'])
    assert filtered_df == [Row('90', '1927', 'M', '2017-01-01', 'dummyval')]

    # save original state of built-in transformer for latter test
    old_transformer = Transformer(**dict(common_emr_priv.emr_transformer.transforms))

    # test with additional transforms
    additional_transformer = Transformer(
        notransform=[TransformFunction(upper, ['notransform'], True)]
    )
    filtered_df = filter_helper(spark, ['100', '1880', 'F', '2017-01-01', 'dummyval'], additional_transformer)
    assert filtered_df == [Row('90', '1927', 'F', '2017-01-01', 'DUMMYVAL')]

    # test original transformer was not modifed by additional transformes dict update
    assert common_emr_priv.emr_transformer.transforms == old_transformer.transforms
    
    # test unknown gender code
    filtered_df = filter_helper(spark, ['100', '1880', 'Unknown', '2017-01-01', 'dummyval'])
    assert filtered_df == [Row('90', '1927', 'U', '2017-01-01', 'dummyval')]
