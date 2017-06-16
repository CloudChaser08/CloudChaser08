import pytest

import spark.providers.diplomat.pharmacyclaims.sparkNormalizeDiplomatRx as diplomat

results = []

def cleanup(spark):
    spark['sqlContext'].dropTempTable('transactions')
    spark['sqlContext'].dropTempTable('pharmacyclaims_common_model')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)
    diplomat.run(spark['spark'], spark['runner'], '2017-05-01', True)
    global results
    results = spark['sqlContext'].sql('select * from pharmacyclaims_common_model') \
                                 .collect()


def test_embedded_commas():
    "Ensure that rows with embedded columns were filtered out"
    assert not filter(lambda r: r.claim_id == 'claim-4', results)
    assert len(results) == 9


def test_cleanup(spark):
    cleanup(spark)
