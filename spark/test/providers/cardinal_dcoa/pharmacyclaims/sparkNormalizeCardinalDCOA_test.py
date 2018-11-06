import pytest

import spark.providers.cardinal_dcoa.pharmacyclaims.sparkNormalizeCardinalDCOA as cardinal_dcoa

results = None

def cleanup(spark):
    spark['sqlContext'].dropTempTable('pharmacyclaims_common_model')
    spark['sqlContext'].dropTempTable('cardinal_dcoa_transactions')


@pytest.mark.usefixtures('spark')
def test_init(spark):
    cleanup(spark)

    global results
    cardinal_dcoa.run(spark['spark'], spark['runner'], '2017-09-25', test=True)
    results = spark['sqlContext'].sql('select * from pharmacyclaims_common_model') \
                                 .collect()


def test_num_rows_is_same_as_transaction():
    assert len(results) == 20


def test_source_record_id_not_null():
    for row in results:
        assert row.record_id is not None


def test_hvid_null():
    for row in results:
        assert row.hvid is None


def test_diagnosis_code_qual_is_99():
    for row in results:
        assert row.diagnosis_code_qual == '99'


def test_product_service_id_qual_is_99():
    for row in results:
        assert row.product_service_id_qual == '99'


def test_pharmacy_other_qual_is_99():
    for row in results:
        assert row.pharmacy_other_qual == '99'


def test_prov_prescribing_qual_is_99():
    for row in results:
        assert row.prov_prescribing_qual == '99'


def test_prov_primary_care_qual_is_99():
    for row in results:
        assert row.prov_primary_care_qual == '99'


def test_discharge_date_filled():
    for row in results:
        assert row.discharge_date is not None

def test_product_code_qualifier():
    for row in results:
        assert '99' == row.original_product_code_qualifier
        assert '88' == row.original_product_code


def test_cleanup(spark):
    cleanup(spark)
