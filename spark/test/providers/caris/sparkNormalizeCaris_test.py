import pytest

import datetime

import spark.providers.caris.sparkNormalizeCaris as caris

results = []


@pytest.mark.usefixtures("spark")
def test_init(spark):
    # new model run
    caris.run(spark['spark'], spark['runner'], '2017-05-01', True)
    global results
    results = spark['sqlContext'].sql('select * from lab_common_model') \
                                 .collect()


def test_date_parsing():
    assert filter(lambda r: r.claim_id == 'patient-0_deid-0', results)[0] \
        .date_service == datetime.date(2017, 3, 25)


def test_labtests_translated():
    assert len(filter(
        lambda r: r.claim_id == 'patient-1_deid-1' and r.test_ordered_name == 'NGS_OFFERING',
        results
    ))


def test_cleanup(spark):
    spark['sqlContext'].dropTempTable('lab_common_model')
