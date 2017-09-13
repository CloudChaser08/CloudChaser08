import pytest

from pyspark.sql.types import StructType, StructField, StringType

import spark.delivery.foresite_hv000321.sparkExtractForesite as foresite


@pytest.mark.usefixtures("spark")
def test_init(spark):
    df = spark['spark'].sparkContext.parallelize([
        ['metformin_ndc', 'metformin', 'metformin_brand']
    ]).toDF(
        StructType([
            StructField('ndc_code', StringType(), True),
            StructField('nonproprietary_name', StringType(), True),
            StructField('proprietary_name', StringType(), True)
        ])
    )

    foresite.run(spark['spark'], spark['runner'], '2017-05-01', True)


