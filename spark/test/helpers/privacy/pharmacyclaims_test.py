import pytest
import spark.helpers.privacy.pharmacyclaims as pharmacy_priv
from pyspark.sql.types import StructField, StructType, StringType, Row

@pytest.mark.usefixtures("spark")
def test_filter(spark):
    # test df including commonly filtered fields
    test_df = spark['spark'].sparkContext.parallelize([
        ['1', 'rxnum', 'notransform']
    ]).toDF(StructType([
        StructField('id', StringType()),
        StructField('rx_number', StringType()),
        StructField('notransform', StringType())
    ]))

    # assert privacy filtering is being applied
    assert pharmacy_priv.filter(test_df).collect() \
        == [Row('1', '7e13d20348e5c163f11944938f1984f9', 'notransform')]
