import pytest
import spark.helpers.privacy.events as events_priv
from pyspark.sql.types import StructField, StructType, StringType, Row

@pytest.mark.usefixtures("spark")
def test_filter(spark):
    # test df including commonly filtered fields
    test_df = spark['spark'].sparkContext.parallelize([
        ['1', '18', '2017-01-01', '1999'],
        ['2', '98', '2017-01-01', '1919']
    ]).toDF(StructType([
        StructField('id', StringType()),
        StructField('patient_age', StringType()),
        StructField('event_date', StringType()),
        StructField('patient_year_of_birth', StringType())
    ]))

    # assert privacy filtering is being applied
    assert events_priv.filter(test_df).collect() \
        == [Row('1', '18', '2017-01-01', '1999'),
            Row('2', '90', '2017-01-01', '1927')]
