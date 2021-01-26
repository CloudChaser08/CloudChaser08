import pytest
import spark.helpers.privacy.labtests as lab_priv
from pyspark.sql.types import StructField, StructType, StringType, Row


@pytest.mark.usefixtures("spark")
def test_filter(spark):
    # test df including commonly filtered fields
    test_df = spark['spark'].sparkContext.parallelize([
        ['010101-01'],
        ['---01-01--'],
        [None],
        ['ABC01B-']
    ]).toDF(StructType([
        StructField('loinc_code', StringType())
    ]))

    # assert privacy filtering is being applied
    assert lab_priv.filter(test_df).collect() \
        == [Row('01010101'), Row('0101'), Row(None), Row('01')]
