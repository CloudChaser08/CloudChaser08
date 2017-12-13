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

def test_cleanup(spark):
    cleanup(spark)
