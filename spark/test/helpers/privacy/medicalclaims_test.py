import pytest
import spark.helpers.privacy.medicalclaims as medical_priv
from pyspark.sql.types import StructField, StructType, StringType, Row

@pytest.mark.usefixtures("spark")
def test_filter(spark):
    # test df including commonly filtered fields
    test_df = spark['spark'].sparkContext.parallelize([
        ['1', 'V24', '02', '2017-01-01', 'notransform'],
        ['2', 'V24', '01', '2015-01-01', 'notransform'],
        ['3', 'V24', None, '2017-01-01', 'notransform']
    ]).toDF(StructType([
        StructField('id', StringType()),
        StructField('diagnosis_code', StringType()),
        StructField('diagnosis_code_qual', StringType()),
        StructField('date_service', StringType()),
        StructField('notransform', StringType())
    ]))

    # assert privacy filtering is being applied
    assert medical_priv.filter(test_df).collect() \
        == [Row('1', None, '02', '2017-01-01', 'notransform'),
            Row('2', 'V24', '01', '2015-01-01', 'notransform'),
            Row('3', None, None, '2017-01-01', 'notransform')]
