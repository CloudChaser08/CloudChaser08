import pytest

from pyspark.sql.types import StructField, StructType, StringType

from spark.qa.datafeed import Datafeed
import spark.qa.datatypes as types
import spark.qa.checks.checks_test as checks_test

sample_datafeed = None

@pytest.mark.usefixtures("spark")
def test_test_valid_gender_values_in_target(spark):
    working = Datafeed(
        datatype=types.MEDICALCLAIMS,
        target_data=spark['spark'].sparkContext.parallelize([
            ['F'],
            ['M'],
            ['U'],
            [None]
        ]).toDF(StructType([
            StructField('patient_gender', StringType())
        ]))
    )

    broken = Datafeed(
        datatype=types.MEDICALCLAIMS,
        target_data=spark['spark'].sparkContext.parallelize([
            ['INVALID'],
            ['M'],
            ['U'],
            [None]
        ]).toDF(StructType([
            StructField('patient_gender', StringType())
        ]))
    )

    checks_test.test_valid_gender_values_in_target(working)

    with pytest.raises(AssertionError):
        checks_test.test_valid_gender_values_in_target(broken)


def test_test_valid_patient_state_values_in_target(spark):
    working = Datafeed(
        datatype=types.MEDICALCLAIMS,
        target_data=spark['spark'].sparkContext.parallelize([
            ['PA'],
            ['VT'],
            ['NJ'],
            [None]
        ]).toDF(StructType([
            StructField('patient_state', StringType())
        ]))
    )

    broken = Datafeed(
        datatype=types.MEDICALCLAIMS,
        target_data=spark['spark'].sparkContext.parallelize([
            ['INVALID'],
            ['PA'],
            ['NJ'],
            [None]
        ]).toDF(StructType([
            StructField('patient_state', StringType())
        ]))
    )

    checks_test.test_valid_patient_state_values_in_target(working)

    with pytest.raises(AssertionError):
        checks_test.test_valid_patient_state_values_in_target(broken)
