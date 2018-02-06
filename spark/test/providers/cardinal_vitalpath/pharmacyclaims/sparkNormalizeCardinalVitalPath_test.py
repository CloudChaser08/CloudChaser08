import pytest

import spark.providers.cardinal_vitalpath.pharmacyclaims.sparkNormalizeCardinalVitalPath as cardinal_vitalpath

results = None

@pytest.mark.usefixtures('spark')
def test_init(spark):
    global results
    cardinal_vitalpath.run(spark['spark'], spark['runner'], '2017-10-30', test=True)
    results = spark['sqlContext'].sql('select * from pharmacyclaims_common_model') \
                                 .collect()


def test_ndc_codes_cleaned_up():
    for row in results:
        assert row.ndc_code.isdigit()


def test_no_rows_dropped():
    assert len(results) == 10


def test_hardcode_values():
    for row in results:
        assert row.data_feed == '30'
        assert row.data_vendor == '42'
        assert row.model_version == '6'


def test_procedure_code_is_upper_case():
    for row in results:
        assert row.procedure_code.isupper()


def test_unit_of_measure_is_upper():
    for row in results:
        assert row.unit_of_measure.isupper()
