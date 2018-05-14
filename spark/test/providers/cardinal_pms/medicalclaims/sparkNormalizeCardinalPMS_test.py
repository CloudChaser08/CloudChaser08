import pytest

import logging

import spark.providers.cardinal_pms.medical_claims.sparkNormalizeCardinalPMS as cardinal_pms

results = []

def cleanup(spark):
    spark['sqlContext'].dropTempTable('medicalclaims_common_model')
    spark['sqlContext'].dropTempTable('transactional_cardinal_pms')
    spark['sqlContext'].dropTempTable('service_line_exploder')
    spark['sqlContext'].dropTempTable('claim_exploder')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cardinal_pms.run(spark['spark'], spark['runner'], '1990-01-01', test=True)
    global results
    results = spark['sqlContext'].sql('select * from medicalclaims_common_model') \
                                 .collect()


def test_claim_levels_all_populated():
    assert len(filter(lambda r: r.service_line_number is None, results)) == 5


def test_service_line_levels_all_populated():
    assert len(filter(lambda r: r.service_line_number is not None, results)) == 27


def test_claim_levels_are_unique():
    claim_diags = filter(lambda r: r.service_line_number is None, results)
    unique_claim_diags = set(map(lambda r: (r.claim_id, r.diagnosis_code), claim_diags))

    assert len(claim_diags) == len(unique_claim_diags)


def test_vendor_org_id_is_populated():
    for row in results:
        assert row.vendor_org_id == 'z'


def test_cleanup(spark):
    cleanup(spark)


