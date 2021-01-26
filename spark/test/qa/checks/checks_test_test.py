import pytest

from pyspark.sql.types import StructField, StructType, StringType

import spark.qa.datafeed as qa_datafeed
import spark.qa.checks.checks_test as checks_test

sample_datafeed = None


@pytest.mark.usefixtures("spark")
def test_gender_validation(spark):
    working = qa_datafeed.standard_medicalclaims_datafeed(
        target_data=spark['spark'].sparkContext.parallelize([
            ['F'],
            ['M'],
            ['U'],
            [None]
        ]).toDF(StructType([
            StructField('patient_gender', StringType())
        ]))
    )

    broken = qa_datafeed.standard_medicalclaims_datafeed(
        target_data=spark['spark'].sparkContext.parallelize([
            ['INVALID'],
            ['M'],
            ['U'],
            [None]
        ]).toDF(StructType([
            StructField('patient_gender', StringType())
        ]))
    )

    checks_test.test_validations(working, qa_datafeed.gender_validation('patient_gender'))

    with pytest.raises(AssertionError):
        checks_test.test_validations(broken, qa_datafeed.gender_validation('patient_gender'))


def test_state_validation(spark):
    working = qa_datafeed.standard_medicalclaims_datafeed(
        target_data=spark['spark'].sparkContext.parallelize([
            ['PA'],
            ['VT'],
            ['NJ'],
            [None]
        ]).toDF(StructType([
            StructField('patient_state', StringType())
        ]))
    )

    broken = qa_datafeed.standard_medicalclaims_datafeed(
        target_data=spark['spark'].sparkContext.parallelize([
            ['INVALID'],
            ['PA'],
            ['NJ'],
            [None]
        ]).toDF(StructType([
            StructField('patient_state', StringType())
        ]))
    )

    checks_test.test_validations(working, qa_datafeed.state_validation('patient_state'))

    with pytest.raises(AssertionError):
        checks_test.test_validations(broken, qa_datafeed.state_validation('patient_state'))


def test_age_validation(spark):
    working = qa_datafeed.standard_medicalclaims_datafeed(
        target_data=spark['spark'].sparkContext.parallelize([
            ['1'],
            ['84'],
            ['55'],
            [None]
        ]).toDF(StructType([
            StructField('patient_age', StringType())
        ]))
    )

    broken_negative = qa_datafeed.standard_medicalclaims_datafeed(
        target_data=spark['spark'].sparkContext.parallelize([
            ['-1'],
            ['11'],
            [None]
        ]).toDF(StructType([
            StructField('patient_age', StringType())
        ]))
    )

    broken_uncapped = qa_datafeed.standard_medicalclaims_datafeed(
        target_data=spark['spark'].sparkContext.parallelize([
            ['-1'],
            ['11'],
            [None]
        ]).toDF(StructType([
            StructField('patient_age', StringType())
        ]))
    )

    checks_test.test_validations(working, qa_datafeed.age_validation('patient_age'))

    with pytest.raises(AssertionError):
        checks_test.test_validations(broken_negative, qa_datafeed.age_validation('patient_age'))

    with pytest.raises(AssertionError):
        checks_test.test_validations(broken_uncapped, qa_datafeed.age_validation('patient_age'))


def test_full_fill_rates(spark):
    working = qa_datafeed.standard_medicalclaims_datafeed(
        target_data=spark['spark'].sparkContext.parallelize([
            ['full'],
            ['full'],
            ['full'],
        ]).toDF(StructType([
            StructField('record_id', StringType())
        ]))
    )

    broken = qa_datafeed.standard_medicalclaims_datafeed(
        target_data=spark['spark'].sparkContext.parallelize([
            ['full'],
            ['full'],
            ['full'],
            [None]
        ]).toDF(StructType([
            StructField('record_id', StringType())
        ]))
    )

    checks_test.test_full_fill_rates(working, 'record_id')

    with pytest.raises(AssertionError):
        checks_test.test_full_fill_rates(broken, 'record_id')


def test_unique_val_comparison(spark):
    base = spark['spark'].sparkContext.parallelize([
        ['1'],
        ['2'],
        ['3'],
        ['4'],
        ['5'],
    ]).toDF(StructType([
        StructField('record_id', StringType())
    ]))

    working = qa_datafeed.standard_medicalclaims_datafeed(
        target_data=base,
        source_data={
            'src_table': spark['spark'].sparkContext.parallelize([
                ['1'],
                ['2'],
                ['3'],
                ['4'],
                ['5'],
            ]).toDF(StructType([
                StructField('record_id', StringType())
            ]))
        }
    )

    missing_src = qa_datafeed.standard_medicalclaims_datafeed(
        target_data=base,
        source_data={
            'src_table': spark['spark'].sparkContext.parallelize([
                ['1'],
                ['2'],
                ['3'],
                ['4'],
            ]).toDF(StructType([
                StructField('record_id', StringType())
            ]))
        }
    )

    missing_target = qa_datafeed.standard_medicalclaims_datafeed(
        target_data=base,
        source_data={
            'src_table': spark['spark'].sparkContext.parallelize([
                ['1'],
                ['2'],
                ['3'],
                ['4'],
                ['5'],
                ['6'],
            ]).toDF(StructType([
                StructField('record_id', StringType())
            ]))
        }
    )

    comparison = qa_datafeed.Comparison('src_table.record_id', 'record_id')

    checks_test.test_all_unique_vals_in_src_and_target(working, comparison)

    with pytest.raises(AssertionError):
        checks_test.test_all_unique_vals_in_src_and_target(missing_target, comparison)

    with pytest.raises(AssertionError):
        checks_test.test_all_unique_vals_in_src_and_target(missing_src, comparison)
