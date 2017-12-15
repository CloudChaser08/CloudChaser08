import pdb
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
    print len(results)
    pdb.set_trace()


def test_claim_levels_all_populated():
    assert len(filter(lambda r: r.service_line_number is None, results)) == 23


def test_service_line_levels_all_populated():
    assert len(filter(lambda r: r.service_line_number is not None, results)) == 27


def test_cleanup(spark):
    cleanup(spark)


